use tokio::net::UnixStream;
use tokio::io::AsyncWriteExt;
use serde::Serialize;
use serde_json;
use std::io;

/// MEV봇으로 전송할 데이터(tx_data)를 직렬화하여, 지정된 Unix 소켓 경로로 전송합니다.
pub async fn send_transactions<T: Serialize>(tx_data: &T, socket_path: &str) -> io::Result<()> {
    // Unix 도메인 소켓에 비동기 연결
    let mut stream = UnixStream::connect(socket_path).await?;
    // 데이터 직렬화 (JSON 포맷)
    let data = serde_json::to_vec(tx_data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    stream.write_all(&data).await?;
    stream.flush().await?;
    Ok(())
}
