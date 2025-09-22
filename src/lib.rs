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
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration, str::FromStr};
use tokio::net::{TcpListener, UnixDatagram};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct Config {
    pub ws_addr: SocketAddr,
    pub ws_path: PathBuf,
    pub socket_path: PathBuf,
    pub buffer_size: usize,
    pub channel_capacity: usize,
    pub log_level: String,
}

struct AppState {
    tx: broadcast::Sender<Message>,
}

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (tx, _) = broadcast::channel::<Message>(config.channel_capacity);
    let app_state = Arc::new(AppState { tx: tx.clone() });
    tokio::spawn(datagram_listener(
        config.socket_path,
        tx.clone(),
        config.buffer_size,
    ));

    let ws_route = {
        let mut p = config.ws_path.to_string_lossy().to_string();
        if !p.starts_with('/') {
            p.insert(0, '/');
        }
        p
    };
    let app = Router::new()
        .route(&ws_route, get(websocket_handler))
        .with_state(app_state);
    info!(
        "WebSocket service listening on ws://{}{}",
        config.ws_addr, ws_route
    );

    let listener = TcpListener::bind(&config.ws_addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(tx))
    .await?;

    Ok(())
}

async fn shutdown_signal(tx: broadcast::Sender<Message>) {
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
}

async fn datagram_listener(path: PathBuf, tx: broadcast::Sender<Message>, buffer_size: usize) {
    if path.exists() {
        if let Err(e) = std::fs::remove_file(&path) {
            error!(
                "Failed to remove existing socket file '{}': {}",
                path.display(),
                e
            );
            return;
        }
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
        if let Ok((len, _addr)) = socket.recv_from(&mut buf).await {
            debug!(
                "Received {} bytes from Unix socket, queued to broadcast {}.",
                len,
                tx.len()
            );
            if tx.receiver_count() > 0 {
                if tx
                    .send(Message::Binary(buf[..len].to_vec().into()))
                    .is_err()
                {
                    warn!("Failed to send message to WebSocket clients.");
                }
            }
        }
    }
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    headers: header::HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let client_addr = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .and_then(|s| SocketAddr::from_str(s.trim()).ok())
        .unwrap_or(addr);

    ws.on_upgrade(move |socket| handle_socket(socket, state, client_addr))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    info!("New WebSocket client connected: {}", addr);

    let mut rx = state.tx.subscribe();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if socket.send(Message::Ping(Default::default())).await.is_err() {
                    info!("Client {} failed to respond to ping, disconnecting.", addr);
                    break;
                }
            }
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        if let Message::Close(_) = &msg {
                            break;
                        }
                        if socket.send(msg).await.is_err() {
                            info!("Client disconnected from {}. Stopping message forward.", addr);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        warn!("Client {} is lagging behind, disconnecting.", addr);
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            Some(Ok(msg)) = socket.recv() => {
                 if let Message::Close(_) = msg {
                     info!("Client {} requested connection close.", addr);
                     break;
                 }
                 if let Message::Pong(_) = msg {
                    continue;
                 }
                 debug!("Received message from client {}, size: {}", addr, msg.into_data().len());
            }
            else => break,
        }
    }

    info!("WebSocket client {} disconnected.", addr);
}
