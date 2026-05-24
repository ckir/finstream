use tokio::task::JoinHandle;

/// Handle to all running provider tasks.
/// Drop or call [`shutdown`] to abort them.
pub struct FinStreamClient {
    pub(crate) handles: Vec<JoinHandle<()>>,
}

impl FinStreamClient {
    /// Abort all provider tasks immediately.
    pub fn shutdown(self) {
        for handle in self.handles {
            handle.abort();
        }
    }

    /// Wait for all provider tasks to finish (they run forever unless aborted).
    pub async fn join(self) {
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}
