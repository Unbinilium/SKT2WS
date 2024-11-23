[Skt2Ws]
    A simple programe that broadcasts messages from a Unix socket to all connected WebSocket clients.

[Build]
    cargo build --release

[Run]
    cargo run --release

[Test]
    Sub(multiple): websocat ws://127.0.0.1:3000/ws
    Pub(single): echo "Hello, World!" | socat - UNIX-CONNECT:/dev/shm/skt2ws.sock
