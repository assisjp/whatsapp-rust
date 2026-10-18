use super::*;
use crate::store::traits::Backend;
use crate::types::message::{MessageInfo, MessageSource};
use crate::{HistorySyncDurabilityHook, HistorySyncKey, HistorySyncMetadata};
use buffa::Message as _;
use flate2::{Compression, write::ZlibEncoder};
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use waproto::whatsapp as wa;

#[derive(Default)]
struct RecordingHistory {
    records: Mutex<Vec<(HistorySyncKey, Vec<u8>)>>,
    notifications: AtomicUsize,
    chunks: AtomicUsize,
    fail: AtomicU8,
    parked: AtomicBool,
    entered: tokio::sync::Notify,
    gate: tokio::sync::Notify,
    backend: Option<Arc<dyn Backend>>,
    require_journal: AtomicBool,
}

#[async_trait::async_trait]
impl HistorySyncDurabilityHook for RecordingHistory {
    async fn on_notification(
        &self,
        key: &HistorySyncKey,
        metadata: &HistorySyncMetadata<'_>,
        notification: &[u8],
    ) -> anyhow::Result<()> {
        self.notifications.fetch_add(1, Ordering::SeqCst);
        if self.require_journal.load(Ordering::SeqCst) {
            assert!(
                self.backend
                    .as_ref()
                    .unwrap()
                    .get_pending_inbound(&key.chat, &key.sender, &key.id)
                    .await?
                    .is_some(),
                "the full carrier must be durable before the host callback"
            );
        }
        let decoded = wa::message::HistorySyncNotification::decode_from_slice(notification)?;
        assert_eq!(
            metadata.sync_type,
            decoded.sync_type.map(|kind| kind as i32)
        );
        assert_eq!(metadata.chunk_order, decoded.chunk_order);
        assert_eq!(metadata.progress, decoded.progress);
        assert_eq!(metadata.file_length, decoded.file_length);
        assert_eq!(
            metadata.peer_data_request_session_id,
            decoded.peer_data_request_session_id.as_deref()
        );
        assert_eq!(
            metadata.inline_payload_len,
            decoded
                .initial_hist_bootstrap_inline_payload
                .as_ref()
                .map(Vec::len)
        );
        anyhow::ensure!(
            self.fail.load(Ordering::SeqCst) != 1,
            "notification failure"
        );
        {
            let mut records = self.records.lock().unwrap();
            if let Some((_, previous)) = records.iter().find(|(previous, _)| previous == key) {
                anyhow::ensure!(previous == notification, "conflicting notification");
            } else {
                records.push((key.clone(), notification.to_vec()));
            }
        }
        if self.parked.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.gate.notified().await;
        }
        Ok(())
    }

    async fn on_compressed_chunk(
        &self,
        key: &HistorySyncKey,
        metadata: &HistorySyncMetadata<'_>,
        compressed: &[u8],
    ) -> anyhow::Result<()> {
        self.chunks.fetch_add(1, Ordering::SeqCst);
        let records = self.records.lock().unwrap();
        let (_, notification) = records
            .iter()
            .find(|(previous, _)| previous == key)
            .unwrap();
        let decoded = wa::message::HistorySyncNotification::decode_from_slice(notification)?;
        assert_eq!(metadata.chunk_order, decoded.chunk_order);
        if let Some(inline) = decoded.initial_hist_bootstrap_inline_payload {
            assert_eq!(compressed, inline);
        }
        anyhow::ensure!(self.fail.load(Ordering::SeqCst) != 2, "chunk failure");
        Ok(())
    }
}

fn notification() -> wa::message::HistorySyncNotification {
    let mut history = wa::HistorySync::default();
    history.sync_type = wa::history_sync::HistorySyncType::RECENT;
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&history.encode_to_vec()).unwrap();
    let compressed = encoder.finish().unwrap();
    let mut notification = wa::message::HistorySyncNotification::default();
    notification.sync_type = Some(wa::message::HistorySyncType::RECENT);
    notification.file_length = Some(compressed.len() as u64);
    notification.chunk_order = Some(7);
    notification.progress = Some(55);
    notification.peer_data_request_session_id = Some("synthetic-recovery-session".into());
    notification.initial_hist_bootstrap_inline_payload = Some(compressed);
    notification
}

fn info(id: &str) -> Arc<MessageInfo> {
    Arc::new(MessageInfo {
        id: id.into(),
        source: MessageSource {
            chat: "15550000001@s.whatsapp.net".parse().unwrap(),
            sender: "15550000001:7@s.whatsapp.net".parse().unwrap(),
            is_from_me: true,
            ..Default::default()
        },
        ..Default::default()
    })
}

fn key(info: &MessageInfo) -> HistorySyncKey {
    HistorySyncKey {
        chat: info.source.chat.to_string(),
        sender: info.source.sender.to_string(),
        id: info.id.to_string(),
    }
}

async fn client(
    backend: Arc<dyn Backend>,
) -> (
    Arc<Client>,
    Arc<RecordingHistory>,
    Arc<crate::transport::mock::CapturingMockTransport>,
) {
    let client = crate::test_utils::create_test_client_with_backend(backend.clone()).await;
    let transport = Arc::new(crate::transport::mock::CapturingMockTransport::new());
    *client.noise_socket.lock().unwrap() = Some(Arc::new(crate::socket::NoiseSocket::new(
        client.runtime.clone(),
        transport.clone(),
        wacore::handshake::NoiseCipher::new(&[0; 32]).unwrap(),
        wacore::handshake::NoiseCipher::new(&[0; 32]).unwrap(),
    )));
    client.set_connected_for_test(true);
    client.is_running.store(true, Ordering::Release);
    client.enter_live_mode_for_tests();
    client
        .persistence_manager
        .process_command(wacore::store::commands::DeviceCommand::SetId(Some(
            "15550000001:1@s.whatsapp.net".parse().unwrap(),
        )))
        .await;
    let hook = Arc::new(RecordingHistory {
        backend: Some(backend),
        require_journal: AtomicBool::new(true),
        ..Default::default()
    });
    client
        .history_sync_durability_hook
        .set(hook.clone())
        .ok()
        .unwrap();
    (client, hook, transport)
}

async fn memory_client() -> (
    Arc<Client>,
    Arc<RecordingHistory>,
    Arc<crate::transport::mock::CapturingMockTransport>,
) {
    client(Arc::new(wacore::store::in_memory::InMemoryBackend::new())).await
}

async fn receive(
    client: &Arc<Client>,
    info: &Arc<MessageInfo>,
    notification: wa::message::HistorySyncNotification,
) -> bool {
    let mut protocol = wa::message::ProtocolMessage::default();
    protocol.r#type = Some(wa::message::protocol_message::Type::HistorySyncNotification);
    protocol.history_sync_notification = buffa::MessageField::some(notification);
    let mut message = wa::Message::default();
    message.protocol_message = buffa::MessageField::some(protocol);
    client
        .handle_decrypted_plaintext(
            "msg",
            wacore::messages::MessageUtils::encode_and_pad(&message),
            2,
            0,
            Default::default(),
            info,
        )
        .await
        .unwrap()
        .delivered
}

async fn pending(client: &Client, info: &MessageInfo) -> Option<Vec<u8>> {
    let key = key(info);
    client
        .persistence_manager
        .backend()
        .get_pending_inbound(&key.chat, &key.sender, &key.id)
        .await
        .unwrap()
}

#[tokio::test]
async fn durable_history_notification_and_chunk_precede_all_receipts() {
    let (client, hook, transport) = memory_client().await;
    let info = info("DURABLE_ORDER");
    hook.parked.store(true, Ordering::SeqCst);
    let operation = tokio::spawn({
        let client = client.clone();
        let info = info.clone();
        async move { receive(&client, &info, notification()).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), hook.entered.notified())
        .await
        .unwrap();
    assert!(pending(&client, &info).await.is_some());
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 0);
    assert!(transport.sent().is_empty());
    hook.gate.notify_one();
    assert!(operation.await.unwrap());
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 1);
    assert_eq!(hook.records.lock().unwrap()[0].0, key(&info));
    assert!(pending(&client, &info).await.is_none());
    crate::test_utils::wait_for_outbound_tasks(&client).await;
    assert!(!transport.sent().is_empty());
}

#[tokio::test]
async fn durable_history_failures_keep_the_carrier_and_retry_on_redelivery() {
    for phase in [1, 2] {
        let (client, hook, transport) = memory_client().await;
        let info = info("DURABLE_REDELIVERY");
        hook.fail.store(phase, Ordering::SeqCst);
        assert!(!receive(&client, &info, notification()).await);
        assert!(pending(&client, &info).await.is_some());
        assert!(transport.sent().is_empty());
        hook.fail.store(0, Ordering::SeqCst);
        assert!(client.ack_or_replay_to_hook(&info).await);
        assert_eq!(hook.notifications.load(Ordering::SeqCst), 2);
        assert!(pending(&client, &info).await.is_none());
    }
}

#[tokio::test]
async fn durable_history_cancellation_keeps_journal_for_a_new_client() {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "wa_history_recovery_{}_{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst),
    ));
    let path = path.to_str().unwrap();
    let backend = Arc::new(crate::store::SqliteStore::open(path).await.unwrap());
    let (first, hook, transport) = client(backend).await;
    let info = info("DURABLE_CANCEL");
    hook.parked.store(true, Ordering::SeqCst);
    let operation = tokio::spawn({
        let client = first.clone();
        let info = info.clone();
        async move { receive(&client, &info, notification()).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), hook.entered.notified())
        .await
        .unwrap();
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    assert!(pending(&first, &info).await.is_some());
    assert!(transport.sent().is_empty());
    first.signal_shutdown_sync();
    drop(first);
    drop(hook);
    let backend = Arc::new(crate::store::SqliteStore::open(path).await.unwrap());
    let (second, hook, _) = client(backend).await;
    assert!(second.ack_or_replay_to_hook(&info).await);
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 1);
    assert!(pending(&second, &info).await.is_none());
    second.signal_shutdown_sync();
    drop(second);
    drop(hook);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn durable_history_host_replay_checks_identity_and_retains_metadata() {
    let (first, hook, _) = memory_client().await;
    let info = info("DURABLE_HOST");
    hook.fail.store(2, Ordering::SeqCst);
    assert!(!receive(&first, &info, notification()).await);
    let (key, bytes) = hook.records.lock().unwrap()[0].clone();
    let (second, recovered, _) = memory_client().await;
    recovered.require_journal.store(false, Ordering::SeqCst);
    second
        .replay_history_sync_notification(&key, &bytes)
        .await
        .unwrap();
    assert_eq!(recovered.chunks.load(Ordering::SeqCst), 1);
    assert_eq!(
        recovered.records.lock().unwrap()[0],
        (key.clone(), bytes.clone())
    );
    for foreign in [
        HistorySyncKey {
            sender: "15550000002:7@s.whatsapp.net".into(),
            ..key.clone()
        },
        HistorySyncKey {
            chat: "15550000002@s.whatsapp.net".into(),
            ..key.clone()
        },
        HistorySyncKey {
            id: String::new(),
            ..key.clone()
        },
    ] {
        assert!(
            second
                .replay_history_sync_notification(&foreign, &bytes)
                .await
                .is_err()
        );
    }
    assert_eq!(recovered.notifications.load(Ordering::SeqCst), 1);
    assert!(
        second
            .replay_history_sync_notification(&key, &[255])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn durable_history_nonself_never_reaches_hooks() {
    let (client, hook, _) = memory_client().await;
    let mut info = (*info("FOREIGN_HISTORY")).clone();
    info.source.is_from_me = false;
    info.source.sender = "15550000002@s.whatsapp.net".parse().unwrap();
    let info = Arc::new(info);
    assert!(receive(&client, &info, notification()).await);
    assert_eq!(hook.notifications.load(Ordering::SeqCst), 0);
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 0);
    assert!(pending(&client, &info).await.is_none());
}

#[tokio::test]
async fn durable_history_malformed_chunk_and_download_failure_stay_unacked() {
    for inline in [Some(vec![1, 2, 3]), None] {
        let (client, hook, transport) = memory_client().await;
        let info = info("INVALID_HISTORY");
        let mut notification = notification();
        notification.initial_hist_bootstrap_inline_payload = inline;
        assert!(!receive(&client, &info, notification).await);
        assert_eq!(hook.notifications.load(Ordering::SeqCst), 1);
        assert!(pending(&client, &info).await.is_some());
        assert!(transport.sent().is_empty());
    }
}

#[tokio::test]
async fn durable_history_same_id_different_sender_has_distinct_identity() {
    let (client, hook, _) = memory_client().await;
    for device in [7, 8] {
        let mut info = (*info("SAME_ID")).clone();
        info.source.sender = format!("15550000001:{device}@s.whatsapp.net")
            .parse()
            .unwrap();
        assert!(receive(&client, &Arc::new(info), notification()).await);
    }
    let records = hook.records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_ne!(records[0].0.sender, records[1].0.sender);
}

#[tokio::test]
async fn durable_history_rejects_conflicting_notification_even_when_projection_is_duplicate() {
    let (client, hook, transport) = memory_client().await;
    let info = info("DIVERGENT_HISTORY");
    assert!(receive(&client, &info, notification()).await);
    crate::test_utils::wait_for_outbound_tasks(&client).await;
    let before = transport.sent().len();
    let mut changed = notification();
    changed.progress = Some(56);
    assert!(!receive(&client, &info, changed).await);
    assert!(pending(&client, &info).await.is_some());
    crate::test_utils::wait_for_outbound_tasks(&client).await;
    assert_eq!(transport.sent().len(), before);
    assert_eq!(hook.notifications.load(Ordering::SeqCst), 2);
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 1);
    assert_eq!(hook.records.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn durable_history_skip_acknowledges_without_capture() {
    let (client, hook, transport) = memory_client().await;
    client.set_skip_history_sync(true);
    assert!(receive(&client, &info("SKIP_HISTORY"), notification()).await);
    assert_eq!(hook.notifications.load(Ordering::SeqCst), 0);
    assert!(!transport.sent().is_empty());
}

#[tokio::test]
async fn durable_history_journal_failure_retains_retry_without_calling_host() {
    let (mut backend, _) = crate::store::release::tests::ProbeBackend::new();
    let failing = Arc::new(AtomicBool::new(false));
    backend.fail_before_method = Some(("replace_pending_inbound_if", failing.clone()));
    let (client, hook, transport) = client(Arc::new(backend)).await;
    let info = info("JOURNAL_FAILURE");
    failing.store(true, Ordering::SeqCst);
    assert!(!receive(&client, &info, notification()).await);
    assert!(pending(&client, &info).await.is_none());
    assert_eq!(hook.notifications.load(Ordering::SeqCst), 0);
    assert!(transport.sent().is_empty());
    assert!(
        !client
            .inbound_commit_batch
            .live_retention
            .for_message(&info)
            .unwrap()
            .is_empty()
    );
    failing.store(false, Ordering::SeqCst);
    assert!(client.ack_or_replay_to_hook(&info).await);
    assert_eq!(hook.notifications.load(Ordering::SeqCst), 1);
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 1);
    assert!(pending(&client, &info).await.is_none());
}

#[tokio::test]
async fn durable_history_ambiguous_journal_write_replays_the_committed_carrier() {
    let backend = Arc::new(wacore::store::in_memory::InMemoryBackend::new());
    let (client, hook, transport) = client(backend.clone()).await;
    let info = info("AMBIGUOUS_HISTORY");
    backend.fail_next_pending_replace_after_commit();
    assert!(!receive(&client, &info, notification()).await);
    assert!(pending(&client, &info).await.is_some());
    assert_eq!(hook.notifications.load(Ordering::SeqCst), 0);
    assert!(transport.sent().is_empty());
    assert!(client.ack_or_replay_to_hook(&info).await);
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 1);
    assert!(pending(&client, &info).await.is_none());
}
