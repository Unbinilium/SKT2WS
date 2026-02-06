use axum::{
    Router,
    extract::{
        State,
        connect_info::ConnectInfo,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::header,
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;
use std::{
    net::SocketAddr, os::unix::fs::PermissionsExt, path::PathBuf, str::FromStr, sync::Arc,
    time::Duration,
};
use tokio::net::{TcpListener, UnixDatagram, UnixListener};
use tokio::sync::{Notify, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn default_ws_path() -> PathBuf {
    PathBuf::from("/ws")
}
fn default_socket_path() -> PathBuf {
    PathBuf::from("/dev/shm/skt2ws.sock")
}
fn default_log_level() -> String {
    "info".into()
}
fn default_buffer_size() -> usize {
    16384
}
fn default_channel_capacity() -> usize {
    4096
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub ws_unix_socket: Option<PathBuf>,
    pub ws_unix_socket_mode: Option<String>,
    pub ws_addr: Option<SocketAddr>,
    #[serde(default = "default_ws_path")]
    pub ws_path: PathBuf,
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

impl Config {
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let data = std::fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&data)?;
        Ok(config)
    }

    pub fn ws_socket_mode(&self) -> Option<u32> {
        self.ws_unix_socket_mode.as_deref().map(|s| {
            u32::from_str_radix(s.strip_prefix("0o").or(s.strip_prefix('0')).unwrap_or(s), 8)
                .unwrap_or_else(|_| panic!("invalid ws_unix_socket_mode: {s}"))
        })
    }
}

struct AppState {
    tx: broadcast::Sender<Message>,
}

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ws_unix_socket = config.ws_unix_socket.clone();
    let ws_socket_mode = config.ws_socket_mode();
    let ws_addr = config.ws_addr;

    if ws_addr.is_none() && ws_unix_socket.is_none() {
        return Err(
            "config error: at least one of 'ws_addr' or 'ws_unix_socket' must be set".into(),
        );
    }

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .ok();

    let (tx, _) = broadcast::channel::<Message>(config.channel_capacity);
    let app_state = Arc::new(AppState { tx: tx.clone() });

    let cancel_token = CancellationToken::new();

    tokio::spawn(datagram_listener(
        config.socket_path,
        tx.clone(),
        config.buffer_size,
        cancel_token.clone(),
    ));

    let ws_route = {
        let mut p = config.ws_path.to_string_lossy().to_string();
        if !p.starts_with('/') {
            p.insert(0, '/');
        }
        p
    };

    // Shared shutdown signal
    let shutdown = Arc::new(Notify::new());
    {
        let shutdown = shutdown.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install CTRL+C signal handler");
            info!("Shutdown signal received. Sending close message to all clients.");
            let close_message = Message::Close(Some(axum::extract::ws::CloseFrame {
                code: axum::extract::ws::close_code::NORMAL,
                reason: "Server is shutting down".into(),
            }));
            if tx.send(close_message).is_err() {
                info!("No clients were connected to send shutdown message.");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            shutdown.notify_waiters();
        });
    }

    // Collect socket paths for cleanup on shutdown
    let ws_unix_socket_cleanup = ws_unix_socket.clone();

    // Optional TCP server
    let tcp_handle = if let Some(addr) = ws_addr {
        let tcp_app = Router::new()
            .route(&ws_route, get(websocket_handler))
            .with_state(app_state.clone());
        let tcp_listener = TcpListener::bind(&addr).await?;
        info!("WebSocket TCP listener on ws://{}{}", addr, ws_route);
        let tcp_shutdown = shutdown.clone();
        let tcp_server = axum::serve(
            tcp_listener,
            tcp_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { tcp_shutdown.notified().await });
        Some(tokio::spawn(async move {
            tcp_server.await.map_err(|e| e.to_string())
        }))
    } else {
        None
    };

    // Optional Unix socket server
    let unix_handle = if let Some(ref uds_path) = ws_unix_socket {
        if uds_path.exists() {
            std::fs::remove_file(uds_path)?;
        }
        let uds_listener = UnixListener::bind(uds_path)?;
        if let Some(mode) = ws_socket_mode {
            std::fs::set_permissions(uds_path, std::fs::Permissions::from_mode(mode))?;
            info!("Set WS unix socket permissions to {:o}", mode);
        }
        info!(
            "WebSocket Unix listener on {}{}",
            uds_path.display(),
            ws_route
        );
        let unix_app = Router::new()
            .route(&ws_route, get(unix_websocket_handler))
            .with_state(app_state);
        let unix_shutdown = shutdown.clone();
        let unix_server = axum::serve(uds_listener, unix_app.into_make_service())
            .with_graceful_shutdown(async move { unix_shutdown.notified().await });
        Some(tokio::spawn(async move {
            unix_server.await.map_err(|e| e.to_string())
        }))
    } else {
        None
    };

    // Await both server handles, collecting errors from both
    let (tcp_result, unix_result) = tokio::join!(
        async {
            match tcp_handle {
                Some(h) => h.await.map_err(|e| e.to_string()).and_then(|r| r),
                None => Ok(()),
            }
        },
        async {
            match unix_handle {
                Some(h) => h.await.map_err(|e| e.to_string()).and_then(|r| r),
                None => Ok(()),
            }
        },
    );

    // Cancel datagram listener (it handles its own socket cleanup on exit)
    cancel_token.cancel();
    // Give the datagram listener a moment to exit and clean up
    tokio::time::sleep(Duration::from_millis(50)).await;
    if let Some(ref uds_path) = ws_unix_socket_cleanup
        && uds_path.exists()
    {
        let _ = std::fs::remove_file(uds_path);
        info!("Cleaned up WS unix socket: {}", uds_path.display());
    }

    if let Err(e) = &tcp_result {
        error!("TCP server error: {e}");
    }
    if let Err(e) = &unix_result {
        error!("Unix server error: {e}");
    }
    tcp_result
        .map_err(|e| e.into())
        .and(unix_result.map_err(|e| e.into()))
}

async fn datagram_listener(
    path: PathBuf,
    tx: broadcast::Sender<Message>,
    buffer_size: usize,
    cancel: CancellationToken,
) {
    if path.exists()
        && let Err(e) = std::fs::remove_file(&path)
    {
        error!(
            "Failed to remove existing socket file '{}': {}",
            path.display(),
            e
        );
        return;
    }

    let socket = match UnixDatagram::bind(&path) {
        Ok(socket) => {
            info!("Unix Datagram listener active at {}", path.display());
            socket
        }
        Err(e) => {
            error!(
                "Failed to bind Unix Datagram socket at '{}': {}",
                path.display(),
                e
            );
            return;
        }
    };

    let mut buf = vec![0; buffer_size];
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Datagram listener shutting down.");
                break;
            }
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, _addr)) => {
                        debug!(
                            "Received {} bytes from Unix socket, queued to broadcast {}.",
                            len,
                            tx.len()
                        );
                        if tx.receiver_count() > 0 && tx.send(Message::from(&buf[..len])).is_err() {
                            warn!("Failed to send message to WebSocket clients.");
                        }
                    }
                    Err(e) => {
                        error!("Unix datagram recv error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    // Clean up socket file on exit
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    headers: header::HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // NOTE: X-Forwarded-For is trusted unconditionally. Only used for logging.
    // If exposed directly to the internet, consider validating against trusted proxy IPs.
    let client_addr = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .and_then(|s| SocketAddr::from_str(s.trim()).ok())
        .unwrap_or(addr);

    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| handle_socket(socket, state, client_addr.to_string()))
}

async fn unix_websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| handle_socket(socket, state, "unix".to_string()))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, client_id: String) {
    info!("New WebSocket client connected: {}", client_id);

    let mut rx = state.tx.subscribe();
    let mut ping_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(10),
        Duration::from_secs(10),
    );
    let mut awaiting_pong = false;
    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if awaiting_pong {
                    info!("Client {} did not respond to ping in time, disconnecting.", client_id);
                    break;
                }
                if socket.send(Message::Ping(Default::default())).await.is_err() {
                    info!("Client {} failed to send ping, disconnecting.", client_id);
                    break;
                }
                awaiting_pong = true;
            }
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        if let Message::Close(_) = &msg {
                            // Forward close frame to client before disconnecting
                            let _ = socket.send(msg).await;
                            break;
                        }
                        if socket.send(msg).await.is_err() {
                            info!("Client disconnected from {}. Stopping message forward.", client_id);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(
                            "Client {} lagged behind by {} messages; skipping to latest.",
                            client_id, skipped
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            Some(Ok(msg)) = socket.recv() => {
                 if let Message::Close(_) = &msg {
                     info!("Client {} requested connection close.", client_id);
                     // Echo close frame back per RFC 6455
                     let _ = socket.send(msg).await;
                     break;
                 }
                 if let Message::Pong(_) = msg {
                    awaiting_pong = false;
                    continue;
                 }
                 debug!("Received message from client {}, size: {}", client_id, msg.into_data().len());
            }
            else => break,
        }
    }

    info!("WebSocket client {} disconnected.", client_id);
}
