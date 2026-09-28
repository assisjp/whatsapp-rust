use super::{NotificationHandler, StanzaHandler};
use crate::GroupNotificationDurabilityHook;
use crate::test_utils::{TestEventCollector, create_iq_test_client, node_to_owned_ref};
use crate::types::events::Event;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use wacore_binary::{Jid, OwnedNodeRef, builder::NodeBuilder};

const GROUP: &str = "120363000000000000@g.us";

struct RecordingHook {
    nodes: Mutex<Vec<Arc<OwnedNodeRef>>>,
    entered: Notify,
    release: Notify,
    fail: bool,
}

impl RecordingHook {
    fn new(fail: bool) -> Self {
        Self {
            nodes: Mutex::new(Vec::new()),
            entered: Notify::new(),
            release: Notify::new(),
            fail,
        }
    }
}

#[async_trait::async_trait]
impl GroupNotificationDurabilityHook for RecordingHook {
    async fn on_notification(&self, node: Arc<OwnedNodeRef>) -> anyhow::Result<()> {
        self.nodes.lock().unwrap().push(node);
        self.entered.notify_one();
        self.release.notified().await;
        if self.fail {
            anyhow::bail!("durable commit rejected");
        }
        Ok(())
    }
}

fn multi_action_node() -> Arc<OwnedNodeRef> {
    node_to_owned_ref(
        &NodeBuilder::new("notification")
            .attr("type", "w:gp2")
            .attr("from", GROUP)
            .attr("id", "group-durability-1")
            .attr("t", "1773519041")
            .children([
                NodeBuilder::new("modify")
                    .children([NodeBuilder::new("participant")
                        .attr("jid", "12025550102@s.whatsapp.net")
                        .build()])
                    .build(),
                NodeBuilder::new("subject")
                    .attr("subject", "renamed")
                    .build(),
            ])
            .build(),
    )
}

fn group_events(collector: &TestEventCollector) -> usize {
    collector
        .events()
        .iter()
        .filter(|event| matches!(&***event, Event::GroupUpdate(_)))
        .count()
}

#[tokio::test]
async fn failed_group_commit_withholds_wire_ack_and_preserves_sender_key_state() {
    let (client, transport) = create_iq_test_client().await;
    let collector = Arc::new(TestEventCollector::default());
    client.subscribe_handler(collector.clone()).detach();
    let participant: Jid = "12025550102@s.whatsapp.net".parse().unwrap();
    client
        .set_sender_key_status_for_devices(GROUP, &[participant], true, false)
        .await
        .unwrap();
    let before = client
        .persistence_manager
        .get_sender_key_devices(GROUP)
        .await
        .unwrap();
    assert!(!before.is_empty());
    let hook = Arc::new(RecordingHook::new(true));
    assert!(
        client
            .group_notification_durability_hook
            .set(hook.clone())
            .is_ok()
    );
    let node = multi_action_node();
    let processing = tokio::spawn({
        let client = client.clone();
        async move { client.process_node(node).await }
    });
    tokio::time::timeout(Duration::from_secs(5), hook.entered.notified())
        .await
        .unwrap();
    assert_eq!(group_events(&collector), 0);
    assert!(
        transport.sent().is_empty(),
        "no ACK while commit is pending"
    );
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), processing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(group_events(&collector), 0);
    assert!(
        transport.sent().is_empty(),
        "failed commit must not queue ACK or NACK"
    );
    let after = client
        .persistence_manager
        .get_sender_key_devices(GROUP)
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "modify must not clear sender-key tracking"
    );
    assert_eq!(hook.nodes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn successful_multi_action_commit_is_once_before_events_and_wire_ack() {
    let (client, transport) = create_iq_test_client().await;
    let collector = Arc::new(TestEventCollector::default());
    client.subscribe_handler(collector.clone()).detach();
    let hook = Arc::new(RecordingHook::new(false));
    assert!(
        client
            .group_notification_durability_hook
            .set(hook.clone())
            .is_ok()
    );
    let node = multi_action_node();
    let original = node.clone();
    let processing = tokio::spawn({
        let client = client.clone();
        async move { client.process_node(node).await }
    });
    tokio::time::timeout(Duration::from_secs(5), hook.entered.notified())
        .await
        .unwrap();
    assert_eq!(group_events(&collector), 0);
    assert!(transport.sent().is_empty());
    {
        let recorded = hook.nodes.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "one hook for the full multi-action envelope"
        );
        assert!(Arc::ptr_eq(&recorded[0], &original));
        assert_eq!(recorded[0].backing_bytes(), original.backing_bytes());
    }
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), processing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(group_events(&collector), 2);
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.sent().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let plaintexts = crate::test_utils::decrypt_wire_frames(&transport.sent(), &[0; 32]);
    let unpacked = wacore_binary::util::unpack(&plaintexts[0]).unwrap();
    let ack = OwnedNodeRef::new(unpacked.into_owned()).unwrap();
    assert_eq!(ack.tag(), "ack");
    assert_eq!(
        ack.get().attrs().optional_string("id").as_deref(),
        Some("group-durability-1")
    );
    assert_eq!(hook.nodes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn default_group_processing_dispatches_all_actions_and_keeps_ack_eligible() {
    let (client, _) = create_iq_test_client().await;
    let collector = Arc::new(TestEventCollector::default());
    client.subscribe_handler(collector.clone()).detach();
    let mut cancelled = false;
    assert!(
        NotificationHandler
            .handle(client, multi_action_node(), &mut cancelled)
            .await
    );
    assert!(!cancelled);
    assert_eq!(group_events(&collector), 2);
}

#[tokio::test]
async fn idless_group_notification_reaches_hook_without_synthetic_identity() {
    let (client, _) = create_iq_test_client().await;
    let hook = Arc::new(RecordingHook::new(false));
    hook.release.notify_one();
    assert!(
        client
            .group_notification_durability_hook
            .set(hook.clone())
            .is_ok()
    );
    let node = node_to_owned_ref(
        &NodeBuilder::new("notification")
            .attr("type", "w:gp2")
            .attr("from", GROUP)
            .children([NodeBuilder::new("subject").attr("subject", "same").build()])
            .build(),
    );
    let mut cancelled = false;
    assert!(
        NotificationHandler
            .handle(client, node.clone(), &mut cancelled)
            .await
    );
    assert!(!cancelled);
    let captured = hook.nodes.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].get().attrs().optional_string("id").is_none());
    assert!(captured[0].get().attrs().optional_string("t").is_none());
    assert_eq!(captured[0].backing_bytes(), node.backing_bytes());
}

#[tokio::test]
async fn non_group_notification_does_not_invoke_group_hook() {
    let (client, _) = create_iq_test_client().await;
    let hook = Arc::new(RecordingHook::new(true));
    assert!(
        client
            .group_notification_durability_hook
            .set(hook.clone())
            .is_ok()
    );
    let node = node_to_owned_ref(
        &NodeBuilder::new("notification")
            .attr("type", "mediaretry")
            .attr("id", "other")
            .build(),
    );
    let mut cancelled = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        assert!(
            NotificationHandler
                .handle(client, node, &mut cancelled)
                .await
        );
    })
    .await
    .unwrap();
    assert!(!cancelled);
    assert!(hook.nodes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn groups_dirty_failure_is_claimed_and_cancels_ack_before_raw_dispatch() {
    let (client, _) = create_iq_test_client().await;
    let collector = Arc::new(TestEventCollector::default());
    client.subscribe_handler(collector.clone()).detach();
    let hook = Arc::new(RecordingHook::new(true));
    hook.release.notify_one();
    assert!(
        client
            .group_notification_durability_hook
            .set(hook.clone())
            .is_ok()
    );
    let node = node_to_owned_ref(
        &NodeBuilder::new("notification")
            .attr("type", "w:gp2")
            .attr("from", "s.whatsapp.net")
            .children([NodeBuilder::new("groups_dirty").build()])
            .build(),
    );
    let mut cancelled = false;
    assert!(
        NotificationHandler
            .handle(client, node, &mut cancelled)
            .await
    );
    assert!(cancelled);
    assert!(collector.events().is_empty());
    assert_eq!(hook.nodes.lock().unwrap().len(), 1);
}
