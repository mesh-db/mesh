//! Bolt's version-negotiation handshake. The client opens a TCP
//! connection and immediately sends:
//!
//! ```text
//!   60 60 B0 17            // preamble magic
//!   XX XX XX XX            // version preference 1
//!   XX XX XX XX            // version preference 2
//!   XX XX XX XX            // version preference 3
//!   XX XX XX XX            // version preference 4
//! ```
//!
//! Each version slot is four bytes — `[0, range, minor, major]` in
//! big-endian. `range` encodes a back-compat window (e.g. "any minor
//! from `minor-range` through `minor`"). A zero slot is "no preference".
//!
//! The server picks the first client-offered version it supports and
//! writes it back as four bytes; writing `00 00 00 00` means "no
//! overlap — disconnect".

use crate::error::{BoltError, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The fixed preamble every Bolt connection starts with.
pub const PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Encode a single Bolt version as a four-byte handshake slot.
/// `range` is how many minor versions below `minor` the server will
/// also accept — Bolt 5.0 is `(5, 0, 0)`, Bolt 4.4 is `(4, 4, 0)`,
/// and "Bolt 4.0 through 4.4" is `(4, 4, 4)`.
pub const fn version_bytes(major: u8, minor: u8, range: u8) -> [u8; 4] {
    [0, range, minor, major]
}

/// Bolt 4.4 — legacy version, kept for compatibility with older drivers.
pub const BOLT_4_4: [u8; 4] = version_bytes(4, 4, 0);
/// Bolt 5.0 — first major 5 version. Adds LOGON/LOGOFF auth split,
/// adds timezone-aware DateTime (tags 0x49/0x69), removes old
/// LocalDateTime tag 0x44 reuse semantics.
pub const BOLT_5_0: [u8; 4] = version_bytes(5, 0, 0);
/// Bolt 5.1 — adds UTC default for DateTime encoding.
pub const BOLT_5_1: [u8; 4] = version_bytes(5, 1, 0);
/// Bolt 5.2 — minor refinements.
pub const BOLT_5_2: [u8; 4] = version_bytes(5, 2, 0);
/// Bolt 5.3 — adds NOTIFICATION_CONFIG in HELLO extras.
pub const BOLT_5_3: [u8; 4] = version_bytes(5, 3, 0);
/// Bolt 5.4 — adds TELEMETRY message.
pub const BOLT_5_4: [u8; 4] = version_bytes(5, 4, 0);

/// All server-supported versions, in preference order (newest first).
pub const SUPPORTED: &[[u8; 4]] = &[BOLT_5_4, BOLT_5_3, BOLT_5_2, BOLT_5_1, BOLT_5_0, BOLT_4_4];

/// True iff `v` is Bolt 4.4. Bolt 4.4 uses distinct struct tags for
/// timezone-aware datetimes (0x46 / 0x66 with local-wall-clock
/// seconds) where Bolt 5.0+ uses 0x49 / 0x69 with UTC seconds. The
/// encoder / decoder both branch on this to emit the right tag and
/// apply the right seconds-semantics transform.
pub fn is_bolt_4_4(v: [u8; 4]) -> bool {
    // Version bytes are [0, range, minor, major]. Bolt 4.4 = major 4, minor 4.
    v[3] == 4 && v[2] == 4
}

/// Read the client's preamble + four version slots, decide which
/// version to speak, and write the agreed version back. Returns the
/// chosen version bytes on success; emits `NoCompatibleVersion` when
/// the client offered nothing we support (after writing `00000000`
/// to the client so it can disconnect cleanly).
///
/// Picks from the full [`SUPPORTED`] list. To restrict the advertised
/// set (e.g. to force a driver to negotiate down to Bolt 4.4 for a
/// matrix-test cell), use [`perform_server_handshake_with`].
pub async fn perform_server_handshake<IO>(io: &mut IO) -> Result<[u8; 4]>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    perform_server_handshake_with(io, SUPPORTED).await
}

/// Same as [`perform_server_handshake`] but picks from `allowed`
/// instead of the full [`SUPPORTED`] list. The driver-matrix harness
/// uses this to clamp the advertised versions per test cell — pinning
/// drivers to specific Bolt versions without depending on driver-side
/// version flags (which most Neo4j drivers don't expose).
pub async fn perform_server_handshake_with<IO>(io: &mut IO, allowed: &[[u8; 4]]) -> Result<[u8; 4]>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut preamble = [0u8; 4];
    io.read_exact(&mut preamble).await?;
    if preamble != PREAMBLE {
        return Err(BoltError::BadPreamble);
    }

    let mut slots = [[0u8; 4]; 4];
    for slot in &mut slots {
        io.read_exact(slot).await?;
    }

    match pick_version_from(&slots, allowed) {
        Some(v) => {
            io.write_all(&v).await?;
            io.flush().await?;
            Ok(v)
        }
        None => {
            io.write_all(&[0, 0, 0, 0]).await?;
            io.flush().await?;
            let raw = [
                u32::from_be_bytes(slots[0]),
                u32::from_be_bytes(slots[1]),
                u32::from_be_bytes(slots[2]),
                u32::from_be_bytes(slots[3]),
            ];
            Err(BoltError::NoCompatibleVersion(raw))
        }
    }
}

/// Write the preamble + four version slots and read the server's
/// agreed version back. Used by tests to drive the server-side code.
pub async fn perform_client_handshake<IO>(
    io: &mut IO,
    preferences: &[[u8; 4]; 4],
) -> Result<[u8; 4]>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    io.write_all(&PREAMBLE).await?;
    for slot in preferences {
        io.write_all(slot).await?;
    }
    io.flush().await?;
    let mut agreed = [0u8; 4];
    io.read_exact(&mut agreed).await?;
    if agreed == [0, 0, 0, 0] {
        let raw = [
            u32::from_be_bytes(preferences[0]),
            u32::from_be_bytes(preferences[1]),
            u32::from_be_bytes(preferences[2]),
            u32::from_be_bytes(preferences[3]),
        ];
        return Err(BoltError::NoCompatibleVersion(raw));
    }
    Ok(agreed)
}

/// Return the first version from `allowed` that the client offered,
/// honoring each slot's `range` field. Empty slots are skipped.
fn pick_version_from(slots: &[[u8; 4]; 4], allowed: &[[u8; 4]]) -> Option<[u8; 4]> {
    for slot in slots {
        if *slot == [0, 0, 0, 0] {
            continue;
        }
        let range = slot[1];
        let minor = slot[2];
        let major = slot[3];
        for offset in 0..=range {
            if offset > minor {
                break;
            }
            let candidate = [0, 0, minor - offset, major];
            if allowed.contains(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn client_and_server_agree_on_4_4() {
        let (mut client, mut server) = duplex(64);

        let server_task = tokio::spawn(async move { perform_server_handshake(&mut server).await });

        let preferences = [BOLT_4_4, [0; 4], [0; 4], [0; 4]];
        let agreed = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap();
        assert_eq!(agreed, BOLT_4_4);
        assert_eq!(server_task.await.unwrap().unwrap(), BOLT_4_4);
    }

    #[tokio::test]
    async fn server_rejects_unsupported_versions() {
        let (mut client, mut server) = duplex(64);

        let server_task = tokio::spawn(async move { perform_server_handshake(&mut server).await });

        // Bolt 6.0 — not supported.
        let preferences = [version_bytes(6, 0, 0), [0; 4], [0; 4], [0; 4]];
        let err = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap_err();
        matches!(err, BoltError::NoCompatibleVersion(_));

        let server_err = server_task.await.unwrap().unwrap_err();
        matches!(server_err, BoltError::NoCompatibleVersion(_));
    }

    #[tokio::test]
    async fn bolt_5_4_preferred_when_offered() {
        let (mut client, mut server) = duplex(64);
        let server_task = tokio::spawn(async move { perform_server_handshake(&mut server).await });

        // Client offers 5.4 + 4.4. Server should pick 5.4 (newer).
        let preferences = [BOLT_5_4, BOLT_4_4, [0; 4], [0; 4]];
        let agreed = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap();
        assert_eq!(agreed, BOLT_5_4);
        assert_eq!(server_task.await.unwrap().unwrap(), BOLT_5_4);
    }

    #[tokio::test]
    async fn bolt_5_range_matches_5_1() {
        let (mut client, mut server) = duplex(64);
        let server_task = tokio::spawn(async move { perform_server_handshake(&mut server).await });

        // "Bolt 5.4 with range 3" → 5.4/5.3/5.2/5.1 accepted. Server picks 5.4.
        let preferences = [version_bytes(5, 4, 3), [0; 4], [0; 4], [0; 4]];
        let agreed = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap();
        assert_eq!(agreed, BOLT_5_4);
        assert_eq!(server_task.await.unwrap().unwrap(), BOLT_5_4);
    }

    #[tokio::test]
    async fn range_negotiation_picks_nearest_supported() {
        let (mut client, mut server) = duplex(64);

        let server_task = tokio::spawn(async move { perform_server_handshake(&mut server).await });

        // "Bolt 4.6 with range 2" → 4.6 / 4.5 / 4.4 accepted. We only
        // support 4.4, which the server should choose.
        let preferences = [version_bytes(4, 6, 2), [0; 4], [0; 4], [0; 4]];
        let agreed = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap();
        assert_eq!(agreed, BOLT_4_4);
        assert_eq!(server_task.await.unwrap().unwrap(), BOLT_4_4);
    }

    #[tokio::test]
    async fn allowed_list_clamps_negotiation_to_4_4() {
        // Client offers 5.4 + 5.0 + 4.4. Server's allowed list contains
        // only 4.4 — verifies the matrix-test cell can pin a driver to
        // a specific Bolt version even though the driver advertises
        // newer ones first.
        let (mut client, mut server) = duplex(64);
        let server_task =
            tokio::spawn(
                async move { perform_server_handshake_with(&mut server, &[BOLT_4_4]).await },
            );
        let preferences = [BOLT_5_4, BOLT_5_0, BOLT_4_4, [0; 4]];
        let agreed = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap();
        assert_eq!(agreed, BOLT_4_4);
        assert_eq!(server_task.await.unwrap().unwrap(), BOLT_4_4);
    }

    #[tokio::test]
    async fn allowed_list_rejects_when_no_overlap() {
        // Client offers only 5.4; server's allowed list is 4.4 only.
        // Negotiation fails — the server writes 0000 and surfaces
        // NoCompatibleVersion to its caller.
        let (mut client, mut server) = duplex(64);
        let server_task =
            tokio::spawn(
                async move { perform_server_handshake_with(&mut server, &[BOLT_4_4]).await },
            );
        let preferences = [BOLT_5_4, [0; 4], [0; 4], [0; 4]];
        let err = perform_client_handshake(&mut client, &preferences)
            .await
            .unwrap_err();
        matches!(err, BoltError::NoCompatibleVersion(_));
        let server_err = server_task.await.unwrap().unwrap_err();
        matches!(server_err, BoltError::NoCompatibleVersion(_));
    }

    #[tokio::test]
    async fn bad_preamble_rejected() {
        let (mut client, mut server) = duplex(64);
        let server_task = tokio::spawn(async move { perform_server_handshake(&mut server).await });
        client.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).await.unwrap();
        // Still send version slots so the server's read_exact for
        // slots doesn't block forever on a closed half.
        for _ in 0..4 {
            client.write_all(&[0; 4]).await.unwrap();
        }
        drop(client);
        let err = server_task.await.unwrap().unwrap_err();
        matches!(err, BoltError::BadPreamble);
    }
}
