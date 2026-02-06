use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::net::UnixDatagram;
use tokio::sync::Barrier;
use tokio_tungstenite::{connect_async, tungstenite::Message};

fn default_ws_url() -> String {
    "ws://127.0.0.1:3000/ws".into()
}
fn default_unix_socket() -> PathBuf {
    PathBuf::from("/dev/shm/skt2ws.sock")
}
fn default_receivers() -> usize {
    4
}
fn default_messages() -> u64 {
    10000
}
fn default_payload_size() -> usize {
    1024
}
fn default_interval_us() -> u64 {
    0
}
fn default_receiver_timeout() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
struct Args {
    /// URL of the running skt2ws WebSocket service (e.g. ws://127.0.0.1:3000/ws)
    #[serde(default = "default_ws_url")]
    ws_url: String,

    /// Path to the Unix datagram socket exposed by skt2ws
    #[serde(default = "default_unix_socket")]
    unix_socket: PathBuf,

    /// Number of WebSocket receivers to attach
    #[serde(default = "default_receivers")]
    receivers: usize,

    /// Number of messages to broadcast through the forwarder
    #[serde(default = "default_messages")]
    messages: u64,

    /// Size of the payload appended after the 16-byte header (in bytes)
    #[serde(default = "default_payload_size")]
    payload_size: usize,

    /// Interval between messages in microseconds (0 = as fast as possible)
    #[serde(default = "default_interval_us")]
    interval_us: u64,

    /// Maximum time to wait for each receiver to finish (seconds)
    #[serde(default = "default_receiver_timeout")]
    receiver_timeout: u64,
}

#[derive(Debug)]
struct ReceiverReport {
    id: usize,
    messages: u64,
    bytes: u64,
    dropped_sequences: u64,
    latency_ns: Vec<u64>,
    first_receive: Option<Duration>,
    last_receive: Option<Duration>,
}

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "benchmark.json".to_string());
    let data = std::fs::read_to_string(&cfg_path)
        .map_err(|e| format!("Failed to read config '{}': {}", cfg_path, e))?;
    let args: Args = serde_json::from_str(&data)
        .map_err(|e| format!("Failed to parse config '{}': {}", cfg_path, e))?;
    if args.receivers == 0 {
        return Err("Receivers count must be at least 1".into());
    }

    let barrier = Arc::new(Barrier::new(args.receivers + 1));
    let start_instant = Instant::now();

    let mut handles = Vec::with_capacity(args.receivers);
    for id in 0..args.receivers {
        let ws_url = args.ws_url.clone();
        let barrier = barrier.clone();
        let expected = args.messages;
        handles.push(tokio::spawn(async move {
            run_receiver(id, ws_url, expected, barrier, start_instant).await
        }));
    }

    barrier.wait().await;

    let send_report = send_messages(&args).await?;

    let receiver_timeout = Duration::from_secs(args.receiver_timeout);
    let mut reports = Vec::with_capacity(args.receivers);
    for handle in handles {
        match tokio::time::timeout(receiver_timeout, handle).await {
            Ok(joined) => match joined {
                Ok(Ok(report)) => reports.push(report),
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(err.into()),
            },
            Err(_) => {
                return Err(format!(
                    "Timeout waiting for receiver task to finish within {}s",
                    args.receiver_timeout
                )
                .into());
            }
        }
    }

    print_summary(&args, &send_report, &reports);

    Ok(())
}

struct SendReport {
    total_messages: u64,
    total_bytes: u64,
    duration: Duration,
}

async fn send_messages(args: &Args) -> Result<SendReport, DynError> {
    let socket = UnixDatagram::unbound()?;
    let mut payload = vec![0u8; args.payload_size];
    for (idx, byte) in payload.iter_mut().enumerate() {
        *byte = idx as u8;
    }

    let mut buffer = vec![0u8; 16 + payload.len()];
    buffer[16..].copy_from_slice(&payload);

    let start = Instant::now();
    for seq in 0..args.messages {
        buffer[..8].copy_from_slice(&seq.to_le_bytes());
        let timestamp_ns = current_time_ns()?;
        buffer[8..16].copy_from_slice(&timestamp_ns.to_le_bytes());

        if let Err(err) = socket.send_to(&buffer, &args.unix_socket).await {
            return Err(format!("Failed to send message {}: {}", seq, err).into());
        }

        if args.interval_us > 0 {
            tokio::time::sleep(Duration::from_micros(args.interval_us)).await;
        }
    }
    let duration = start.elapsed();

    Ok(SendReport {
        total_messages: args.messages,
        total_bytes: (args.messages as usize * buffer.len()) as u64,
        duration,
    })
}

async fn run_receiver(
    id: usize,
    ws_url: String,
    expected_messages: u64,
    barrier: Arc<Barrier>,
    base_instant: Instant,
) -> Result<ReceiverReport, DynError> {
    let (mut ws_stream, _response) = connect_async(&ws_url).await?;
    println!("Receiver {} connected to {}", id, ws_url);
    barrier.wait().await;

    let mut messages = 0u64;
    let mut bytes = 0u64;
    let mut latency_ns = Vec::with_capacity(expected_messages as usize);
    let mut first_receive = None;
    let mut last_receive = None;
    let mut prev_sequence: Option<u64> = None;
    let mut dropped_sequences = 0u64;

    while let Some(item) = ws_stream.next().await {
        let message = match item {
            Ok(msg) => msg,
            Err(err) => return Err(err.into()),
        };

        match message {
            Message::Binary(data) => {
                if data.len() < 16 {
                    continue;
                }
                messages += 1;
                bytes += data.len() as u64;

                let now = Instant::now();
                let since_start = now.duration_since(base_instant);
                if first_receive.is_none() {
                    first_receive = Some(since_start);
                }
                last_receive = Some(since_start);

                let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
                if let Some(prev) = prev_sequence
                    && seq > prev + 1
                {
                    dropped_sequences += seq - prev - 1;
                }
                prev_sequence = Some(seq);

                let send_ns = u64::from_le_bytes(data[8..16].try_into().unwrap());
                let recv_ns = current_time_ns()?;
                latency_ns.push(recv_ns.saturating_sub(send_ns));

                if messages >= expected_messages {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    Ok(ReceiverReport {
        id,
        messages,
        bytes,
        dropped_sequences,
        latency_ns,
        first_receive,
        last_receive,
    })
}

fn current_time_ns() -> Result<u64, DynError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| format!("System clock error: {}", err))?;
    Ok(now.as_nanos() as u64)
}

fn print_summary(args: &Args, send: &SendReport, receivers: &[ReceiverReport]) {
    println!("=== skt2ws benchmark summary ===");
    println!(
        "Sender: {} messages, {} bytes, {:.3} s ({:.2} msg/s, {:.2} MiB/s)",
        send.total_messages,
        send.total_bytes,
        send.duration.as_secs_f64(),
        if send.duration.is_zero() {
            f64::INFINITY
        } else {
            send.total_messages as f64 / send.duration.as_secs_f64()
        },
        if send.duration.is_zero() {
            f64::INFINITY
        } else {
            (send.total_bytes as f64 / (1024.0 * 1024.0)) / send.duration.as_secs_f64()
        }
    );

    if receivers.is_empty() {
        println!("No receiver reports collected.");
        return;
    }

    let mut aggregated_latencies = Vec::new();
    let mut aggregated_messages = 0u64;
    let mut aggregated_bytes = 0u64;
    let mut aggregated_drops = 0u64;

    for report in receivers {
        aggregated_messages += report.messages;
        aggregated_bytes += report.bytes;
        aggregated_drops += report.dropped_sequences;
        aggregated_latencies.extend_from_slice(&report.latency_ns);

        println!("\nReceiver {}:", report.id);
        println!(
            "  messages: {} expected, drops: {}",
            report.messages, report.dropped_sequences
        );
        println!("  bytes: {}", report.bytes);

        if let (Some(first), Some(last)) = (report.first_receive, report.last_receive) {
            let active = last.saturating_sub(first);
            let throughput = if active.is_zero() {
                f64::INFINITY
            } else {
                report.messages as f64 / active.as_secs_f64()
            };
            let mebibytes = report.bytes as f64 / (1024.0 * 1024.0);
            let bandwidth = if active.is_zero() {
                f64::INFINITY
            } else {
                mebibytes / active.as_secs_f64()
            };
            println!("  active window: {:.3} s", active.as_secs_f64());
            println!(
                "  throughput: {:.2} msg/s, {:.2} MiB/s",
                throughput, bandwidth
            );
        }

        if !report.latency_ns.is_empty() {
            let summary = latency_summary(&report.latency_ns);
            println!(
                "  latency (ms): p50={:.3}, p95={:.3}, p99={:.3}, max={:.3}, avg={:.3}",
                summary.p50, summary.p95, summary.p99, summary.max, summary.avg
            );
        }
    }

    println!("\n=== Aggregated receivers ===");
    println!("Total messages: {}", aggregated_messages);
    println!("Total bytes: {}", aggregated_bytes);
    println!("Total drops: {}", aggregated_drops);

    if !aggregated_latencies.is_empty() {
        let summary = latency_summary(&aggregated_latencies);
        println!(
            "Aggregated latency (ms): p50={:.3}, p95={:.3}, p99={:.3}, max={:.3}, avg={:.3}",
            summary.p50, summary.p95, summary.p99, summary.max, summary.avg
        );
    }

    println!("\nParameters:");
    println!("  Receivers: {}", args.receivers);
    println!("  Messages: {}", args.messages);
    println!("  Payload size: {} bytes", args.payload_size);
    println!("  Interval: {} us", args.interval_us);
}

struct LatencySummary {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    avg: f64,
}

fn latency_summary(latencies: &[u64]) -> LatencySummary {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();

    let p50 = nanos_to_ms(percentile(&sorted, 50.0));
    let p95 = nanos_to_ms(percentile(&sorted, 95.0));
    let p99 = nanos_to_ms(percentile(&sorted, 99.0));
    let max = nanos_to_ms(*sorted.last().unwrap() as f64);
    let average_ns = sorted.iter().copied().map(|v| v as f64).sum::<f64>() / sorted.len() as f64;

    LatencySummary {
        p50,
        p95,
        p99,
        max,
        avg: nanos_to_ms(average_ns),
    }
}

fn nanos_to_ms(value: f64) -> f64 {
    value / 1_000_000.0
}

fn percentile(sorted: &[u64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = percentile / 100.0 * (sorted.len() as f64 - 1.0);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower] as f64
    } else {
        let lower_val = sorted[lower] as f64;
        let upper_val = sorted[upper] as f64;
        lower_val + (upper_val - lower_val) * (rank - lower as f64)
    }
}
