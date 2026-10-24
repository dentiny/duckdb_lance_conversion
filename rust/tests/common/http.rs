use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

/// Serve routed test objects, supporting HEAD and a single byte range.
pub async fn serve_http(route: impl Fn(&str) -> Vec<u8> + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut request = String::new();
            while !request.ends_with("\r\n\r\n") {
                if socket.read_line(&mut request).await.unwrap() == 0 {
                    break;
                }
            }
            if request.is_empty() {
                continue;
            }
            let body = route(request.split_whitespace().nth(1).unwrap());
            let range = request.lines().find_map(|line| {
                let line = line.to_ascii_lowercase();
                let (start, end) = line.strip_prefix("range: bytes=")?.split_once('-')?;
                Some(start.parse().unwrap()..end.parse().map_or(body.len(), |end: usize| end + 1))
            });
            let mut response = match &range {
                Some(range) => format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\n",
                    range.start,
                    range.end - 1,
                    body.len()
                ),
                None => "HTTP/1.1 200 OK\r\n".to_owned(),
            };
            let content = &body[range.unwrap_or(0..body.len())];
            response += &format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                content.len()
            );
            let socket = socket.get_mut();
            socket.write_all(response.as_bytes()).await.unwrap();
            if !request.starts_with("HEAD ") {
                socket.write_all(content).await.unwrap();
            }
        }
    });
    format!("http://{address}")
}
