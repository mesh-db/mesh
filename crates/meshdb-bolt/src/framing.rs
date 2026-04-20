//! Bolt's message framing. Every Bolt message sits inside a sequence of
//! "chunks": each chunk is a `u16` big-endian length prefix followed by
//! that many bytes of payload. A zero-length chunk (the two bytes
//! `0x00 0x00`) terminates the message. A chunk length of 0xFFFF is the
//! maximum single-chunk payload size.
//!
//! This module is I/O-agnostic: [`write_message`] writes to a generic
//! `AsyncWrite` and [`read_message`] reads from a generic `AsyncRead`.
//! Higher layers call these once per logical Bolt message after
//! PackStream-encoding the payload.

use crate::error::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum chunk payload size — 2¹⁶ − 1 bytes, the largest `u16` value.
pub const MAX_CHUNK_SIZE: usize = u16::MAX as usize;

/// Write `payload` as one or more chunks followed by the zero-length
/// terminator. Splits oversized payloads into `MAX_CHUNK_SIZE` pieces so
/// arbitrarily large Bolt messages are legal.
pub async fn write_message<W>(writer: &mut W, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining = payload;
    while !remaining.is_empty() {
        let chunk_len = remaining.len().min(MAX_CHUNK_SIZE);
        let len_bytes = (chunk_len as u16).to_be_bytes();
        writer.write_all(&len_bytes).await?;
        writer.write_all(&remaining[..chunk_len]).await?;
        remaining = &remaining[chunk_len..];
    }
    // Zero-length chunk == message terminator.
    writer.write_all(&[0x00, 0x00]).await?;
    writer.flush().await?;
    Ok(())
}

/// Read one complete Bolt message, concatenating chunks until the
/// zero-length terminator arrives. Returns the combined payload — an
/// empty `Vec` indicates the client sent a bare terminator (valid
/// per spec, though unusual).
pub async fn read_message<R>(reader: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut out = Vec::new();
    loop {
        let mut len_buf = [0u8; 2];
        reader.read_exact(&mut len_buf).await?;
        let len = u16::from_be_bytes(len_buf) as usize;
        if len == 0 {
            return Ok(out);
        }
        let start = out.len();
        out.resize(start + len, 0);
        reader.read_exact(&mut out[start..]).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BoltError;
    use tokio::io::duplex;

    #[tokio::test]
    async fn single_chunk_round_trip() {
        let (mut client, mut server) = duplex(4096);
        let payload = b"hello bolt".to_vec();
        write_message(&mut client, &payload).await.unwrap();
        let received = read_message(&mut server).await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn multi_chunk_round_trip() {
        let (mut client, mut server) = duplex(200_000);
        let payload = vec![0xABu8; MAX_CHUNK_SIZE * 2 + 17];
        write_message(&mut client, &payload).await.unwrap();
        let received = read_message(&mut server).await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn empty_message_is_just_terminator() {
        let (mut client, mut server) = duplex(16);
        write_message(&mut client, &[]).await.unwrap();
        let received = read_message(&mut server).await.unwrap();
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn sequential_messages_do_not_bleed() {
        let (mut client, mut server) = duplex(4096);
        write_message(&mut client, b"first").await.unwrap();
        write_message(&mut client, b"second").await.unwrap();
        assert_eq!(read_message(&mut server).await.unwrap(), b"first");
        assert_eq!(read_message(&mut server).await.unwrap(), b"second");
    }

    #[tokio::test]
    async fn truncated_length_prefix_is_error() {
        let (mut client, mut server) = duplex(8);
        // Write one byte, then drop client so read sees EOF.
        client.write_all(&[0x00]).await.unwrap();
        drop(client);
        let err = read_message(&mut server).await.unwrap_err();
        matches!(err, BoltError::Io(_));
    }
}
