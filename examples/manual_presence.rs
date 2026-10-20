//! Opt-in presence maintenance for an unattended companion.
//!
//! Run against an ALREADY PAIRED, dedicated SQLite session directory:
//! `cargo run --example manual_presence -- /path/to/session-directory`
//! The directory must contain `session.sqlite`; do not share it with another
//! running client. Pair using your application's normal UI first.
//!
//! An operator observed a connected, working device listed with stale activity
//! and an expiry warning on the phone. An explicit available -> 45s -> unavailable
//! cycle updated that display. This is empirical, not a documented server rule:
//! 45s and 24h are host choices, not protocol requirements or a guarantee against
//! expiry, logout, QR re-pairing, or notification suppression during the window.
//! Keepalive (#1568) and Manual presence (#1397) solve different problems.
//!
//! This example changes NO SDK defaults or wire formats. It uses the existing
//! presence API, restores unavailable first on every connection, and checkpoints
//! only a cycle completed on one connection. SIGINT/TERM stops and joins the
//! maintenance task before closing the client. A kill/power loss cannot run
//! cleanup; the next connection restores unavailable before any pulse.
//!
//! `PRESENCE_PULSE_SECONDS=1..60` adjusts the available window (default 45).
//! Validate Linked Devices activity over several days AND phone notifications.
//! The phone can suppress notifications during available, even for a short pulse.
//! Interactive hosts must coordinate all other global presence writers; this
//! example exclusively owns those transitions. It does not send chat-state,
//! mark conversations read, or synthesize user traffic.
#[path = "manual_presence/scheduler.rs"]
mod scheduler;

use anyhow::{Context, Result, ensure};
use log::{info, warn};
use scheduler::{Host, Notice, Settings};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::watch;
use whatsapp_rust::PresencePolicy;
use whatsapp_rust::prelude::*;

struct Epoch {
    counter: u64,
    connected: bool,
}
struct Observer {
    epoch: Arc<Mutex<Epoch>>,
    client: Weak<Client>,
}
impl EventHandler for Observer {
    fn handle_event(&self, event: Arc<Event>) {
        let Some(client) = self.client.upgrade() else {
            return;
        };
        let mut epoch = self.epoch.lock().unwrap_or_else(|p| p.into_inner());
        match event.kind() {
            EventKind::Connected => {
                epoch.counter = epoch.counter.saturating_add(1);
                epoch.connected = client.is_socket_connected() && client.is_logged_in();
            }
            EventKind::Disconnected if !client.is_socket_connected() || !client.is_logged_in() => {
                epoch.counter = epoch.counter.saturating_add(1);
                epoch.connected = false;
            }
            EventKind::LoggedOut => {
                epoch.counter = epoch.counter.saturating_add(1);
                epoch.connected = false;
            }
            _ => {}
        }
    }
    fn interest(&self) -> EventInterest {
        EventInterest::of(&[
            EventKind::Connected,
            EventKind::Disconnected,
            EventKind::LoggedOut,
        ])
    }
}

struct Checkpoint {
    directory: PathBuf,
}
impl Checkpoint {
    fn load(&self) -> Result<Option<u64>> {
        match std::fs::read(self.directory.join("presence-success.json")) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn save(&self, timestamp: u64) -> Result<()> {
        let pending = self.directory.join("presence-success.pending");
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&pending)?;
        file.write_all(&serde_json::to_vec(&timestamp)?)?;
        file.sync_all()?;
        std::fs::rename(&pending, self.directory.join("presence-success.json"))?;
        #[cfg(unix)]
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

struct Companion {
    client: Arc<Client>,
    epoch: Arc<Mutex<Epoch>>,
    checkpoint: Arc<Mutex<Checkpoint>>,
}
impl Host for Companion {
    fn generation(&self) -> Option<u64> {
        let epoch = self.epoch.lock().unwrap_or_else(|p| p.into_inner());
        (epoch.connected && self.client.is_socket_connected() && self.client.is_logged_in())
            .then_some(epoch.counter)
    }
    async fn can_pulse(&self) -> bool {
        self.generation().is_some()
    }
    async fn set_available(&self) -> Result<()> {
        self.client.presence().set_available().await?;
        Ok(())
    }
    async fn set_unavailable(&self) -> Result<()> {
        self.client.presence().set_unavailable().await?;
        Ok(())
    }
    async fn load_success(&self) -> Result<Option<u64>> {
        let checkpoint = self.checkpoint.clone();
        tokio::task::spawn_blocking(move || {
            checkpoint.lock().unwrap_or_else(|p| p.into_inner()).load()
        })
        .await?
    }
    async fn save_success(&self, timestamp: u64) -> Result<()> {
        let checkpoint = self.checkpoint.clone();
        // A timed-out spawn_blocking write may still finish. Serialize retries
        // through the same lock instead of racing on the staging file.
        tokio::task::spawn_blocking(move || {
            checkpoint
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .save(timestamp)
        })
        .await?
    }
    fn notice(&self, notice: Notice) {
        match notice {
            Notice::RestoreFailed
            | Notice::AvailableFailed
            | Notice::CheckpointFailed
            | Notice::RecoveryRequested => warn!("presence maintenance: {notice:?}"),
            _ => info!("presence maintenance: {notice:?}"),
        }
    }
    fn recover_transport(&self) {
        // End this example rather than leave an uncertain available session up.
        // A production supervisor can reconnect the same persisted session.
        self.client.signal_shutdown_sync();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let directory = PathBuf::from(
        std::env::args()
            .nth(1)
            .context("provide the dedicated, already-paired session directory")?,
    );
    ensure!(
        directory.join("session.sqlite").is_file(),
        "pair the session before running this example"
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("presence-owner.lock"))?;
    lock.try_lock()
        .context("another example owns this session")?;
    let seconds = std::env::var("PRESENCE_PULSE_SECONDS")
        .unwrap_or_else(|_| "45".into())
        .parse::<u64>()?;
    ensure!(
        (1..=60).contains(&seconds),
        "PRESENCE_PULSE_SECONDS must be 1..60"
    );
    let store = SqliteStore::open(
        directory
            .join("session.sqlite")
            .to_str()
            .context("non-UTF8 session path")?,
    )
    .await?;
    let bot = Bot::builder()
        .with_backend(store)
        .with_presence_policy(PresencePolicy::Manual)
        .build()
        .await?;
    let client = bot.client();
    let epoch = Arc::new(Mutex::new(Epoch {
        counter: 0,
        connected: false,
    }));
    // Synchronous observer: reconnects cannot hide between maintenance ticks.
    let _subscription = client.subscribe_handler(Arc::new(Observer {
        epoch: epoch.clone(),
        client: Arc::downgrade(&client),
    }));
    let host = Companion {
        client: client.clone(),
        epoch,
        checkpoint: Arc::new(Mutex::new(Checkpoint { directory })),
    };
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut handle = bot.spawn();
    let mut maintenance = tokio::spawn(scheduler::run(
        host,
        Settings {
            available_for: Duration::from_secs(seconds),
            ..Settings::default()
        },
        stop_rx,
    ));
    let (bot_finished, maintenance_finished) = tokio::select! {
        _ = shutdown_signal() => (false, false),
        _ = &mut handle => (true, false),
        _ = &mut maintenance => (false, true),
    };
    let _ = stop_tx.send(true);
    if !maintenance_finished {
        if maintenance.await.is_err() {
            warn!("presence task panicked; restoring before transport close");
            let _ =
                tokio::time::timeout(Duration::from_secs(3), client.presence().set_unavailable())
                    .await;
        }
    } else {
        let _ =
            tokio::time::timeout(Duration::from_secs(3), client.presence().set_unavailable()).await;
    }
    client.shutdown().await;
    if !bot_finished {
        handle.await;
    }
    drop(lock);
    Ok(())
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    #[test]
    fn restart_reads_only_a_completed_checkpoint_and_rejects_corruption() {
        let directory =
            std::env::temp_dir().join(format!("presence-example-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let first = Checkpoint {
            directory: directory.clone(),
        };
        assert_eq!(first.load().unwrap(), None);
        // A process that died before rename did not publish success.
        std::fs::write(directory.join("presence-success.pending"), b"123").unwrap();
        assert_eq!(first.load().unwrap(), None);
        first.save(400).unwrap();
        let reopened = Checkpoint {
            directory: directory.clone(),
        };
        assert_eq!(reopened.load().unwrap(), Some(400));
        std::fs::write(directory.join("presence-success.json"), b"invalid").unwrap();
        assert!(reopened.load().is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn duplicate_example_owner_cannot_acquire_same_session_lock() {
        let path = std::env::temp_dir().join(format!("presence-owner-{}", uuid::Uuid::new_v4()));
        let first = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let second = OpenOptions::new().write(true).open(&path).unwrap();
        first.try_lock().unwrap();
        assert!(second.try_lock().is_err());
        drop(first);
        second.try_lock().unwrap();
        drop(second);
        std::fs::remove_file(path).unwrap();
    }
}
