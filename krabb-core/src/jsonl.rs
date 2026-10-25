use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Error, Result};

pub const MAX: usize = 8 * 1024 * 1024;

pub async fn read<T>(input: &mut (impl AsyncBufRead + Unpin), max: usize) -> Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    let mut line = Vec::new();
    let count = input.take((max as u64).saturating_add(1)).read_until(b'\n', &mut line).await?;

    if count == 0 {
        return Ok(None);
    }

    if line.len() > max {
        return Err(Error::Protocol(format!("Frame exceeds {max} bytes.")));
    }

    if line.last() != Some(&b'\n') {
        return Err(Error::Protocol("Frame is missing a newline.".into()));
    }

    let line = std::str::from_utf8(&line).map_err(|error| Error::Protocol(error.to_string()))?;
    Ok(Some(serde_json::from_str(line.trim_end())?))
}

pub async fn write<T>(output: &mut (impl AsyncWrite + Unpin), value: &T) -> Result<()>
where
    T: serde::Serialize,
{
    let line = serde_json::to_string(value)?;

    if line.len().saturating_add(1) > MAX {
        return Err(Error::Protocol(format!("Frame exceeds {MAX} bytes.")));
    }

    output.write_all(line.as_bytes()).await?;
    output.write_all(b"\n").await?;
    output.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::{read, write};

    #[tokio::test]
    async fn round_trips_jsonl() {
        let mut bytes = Vec::new();
        write(&mut bytes, &serde_json::json!({"ok": true})).await.unwrap();
        let mut input = BufReader::new(bytes.as_slice());
        let value: serde_json::Value = read(&mut input, 1024).await.unwrap().unwrap();

        assert_eq!(value["ok"], true);
    }

    #[tokio::test]
    async fn rejects_large_frames() {
        let mut input = BufReader::new(
            br#"{"long":true}
"#
            .as_slice(),
        );

        let result: crate::Result<Option<serde_json::Value>> = read(&mut input, 2).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn rejects_frames_without_newlines() {
        let mut input = BufReader::new(br#"{"ok":true}"#.as_slice());
        let result: crate::Result<Option<serde_json::Value>> = read(&mut input, 1024).await;

        assert!(result.unwrap_err().to_string().contains("newline"));
    }

    #[tokio::test]
    async fn rejects_invalid_utf8() {
        let mut input = BufReader::new([0xff, b'\n'].as_slice());
        let result: crate::Result<Option<serde_json::Value>> = read(&mut input, 1024).await;

        assert!(result.is_err());
    }
}
