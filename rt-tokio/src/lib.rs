use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;

use openraft_rt::AsyncRuntime;
use openraft_rt::OptionalSend;
use rand::RngCore;
use rand::rngs::SmallRng;

mod instant;
mod mpsc;
mod mutex;
mod oneshot;
mod watch;

pub use instant::TokioInstant;
pub use mpsc::TokioMpsc;
pub use mpsc::TokioMpscReceiver;
pub use mpsc::TokioMpscSender;
pub use mpsc::TokioMpscWeakSender;
pub use mutex::TokioMutex;
pub use oneshot::TokioOneshot;
pub use oneshot::TokioOneshotSender;
pub use watch::TokioWatch;
pub use watch::TokioWatchReceiver;
pub use watch::TokioWatchSender;

tokio::task_local! {
    pub static DETERMINISTIC_RNG: RefCell<SmallRng>;
}

pub enum RngSelector {
    Thread(rand::rngs::ThreadRng),
    Deterministic(SmallRng),
}

impl RngCore for RngSelector {
    fn next_u32(&mut self) -> u32 {
        match self {
            RngSelector::Thread(r) => r.next_u32(),
            RngSelector::Deterministic(r) => r.next_u32(),
        }
    }

    fn next_u64(&mut self) -> u64 {
        match self {
            RngSelector::Thread(r) => r.next_u64(),
            RngSelector::Deterministic(r) => r.next_u64(),
        }
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        match self {
            RngSelector::Thread(r) => r.fill_bytes(dest),
            RngSelector::Deterministic(r) => r.fill_bytes(dest),
        }
    }
}

/// `Tokio` is the default asynchronous executor.
pub struct TokioRuntime {
    rt: tokio::runtime::Runtime,
    #[cfg(feature = "single-threaded")]
    local: tokio::task::LocalSet,
}

impl std::fmt::Debug for TokioRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioRuntime").finish()
    }
}

impl AsyncRuntime for TokioRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = tokio::time::Sleep;
    type Instant = TokioInstant;
    type TimeoutError = tokio::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = tokio::time::Timeout<T>;
    type ThreadLocalRng = RngSelector;

    #[inline]
    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        #[cfg(feature = "single-threaded")]
        {
            tokio::task::spawn_local(future)
        }
        #[cfg(not(feature = "single-threaded"))]
        {
            tokio::task::spawn(future)
        }
    }

    #[inline]
    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }

    #[inline]
    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        tokio::time::sleep_until(deadline.0)
    }

    #[inline]
    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        tokio::time::timeout(duration, future)
    }

    #[inline]
    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        tokio::time::timeout_at(deadline.0, future)
    }

    #[inline]
    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    #[inline]
    fn thread_rng() -> Self::ThreadLocalRng {
        if let Ok(rng) = DETERMINISTIC_RNG.try_with(|r| r.borrow().clone()) {
            RngSelector::Deterministic(rng)
        } else {
            RngSelector::Thread(rand::rng())
        }
    }

    type Mpsc = TokioMpsc;
    type Watch = TokioWatch;
    type Oneshot = TokioOneshot;
    type Mutex<T: OptionalSend + 'static> = TokioMutex<T>;

    fn new(threads: usize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        TokioRuntime {
            rt,
            #[cfg(feature = "single-threaded")]
            local: tokio::task::LocalSet::new(),
        }
    }

    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
        T: OptionalSend,
    {
        #[cfg(feature = "single-threaded")]
        {
            self.local.block_on(&self.rt, future)
        }
        #[cfg(not(feature = "single-threaded"))]
        {
            self.rt.block_on(future)
        }
    }
}

#[cfg(test)]
mod tests {
    use openraft_rt::testing::Suite;

    use super::*;

    #[test]
    fn test_tokio_rt() {
        TokioRuntime::run(Suite::<TokioRuntime>::test_all());
    }
}
