use crate::types::history_sync_admission::HistorySyncMetadata;
use anyhow::Result;

/// The identity of one self-sent history notification. Stanza IDs alone are
/// not globally unique; consumers must deduplicate on all three fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistorySyncKey {
    pub chat: String,
    pub sender: String,
    pub id: String,
}

/// A consumer that durably retains both the original notification and its
/// compressed, decrypted history chunk before either receipt is sent.
///
/// Implementations must commit before returning `Ok` and reject a repeat key
/// with different bytes. The library may replay either callback after a crash
/// or a lost acknowledgement. The notification bytes are the complete encoded
/// `wa::message::HistorySyncNotification`, including any inline payload.
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait HistorySyncDurabilityHook: wacore::sync_marker::MaybeSendSync {
    async fn on_notification(
        &self,
        key: &HistorySyncKey,
        metadata: &HistorySyncMetadata<'_>,
        notification: &[u8],
    ) -> Result<()>;

    async fn on_compressed_chunk(
        &self,
        key: &HistorySyncKey,
        metadata: &HistorySyncMetadata<'_>,
        compressed: &[u8],
    ) -> Result<()>;
}
