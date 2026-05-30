use crate::error::TransportError;
use crate::network_availability::NetworkAvailabilityWait;
use crate::network_availability::wait_for_network_availability;
use crate::request::Request;
use rand::Rng;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u64,
    pub base_delay: Duration,
    pub retry_on: RetryOn,
}

#[derive(Debug, Clone)]
pub struct RetryOn {
    pub retry_429: bool,
    pub retry_5xx: bool,
    pub retry_transport: bool,
}

impl RetryOn {
    pub fn should_retry(&self, err: &TransportError, attempt: u64, max_attempts: u64) -> bool {
        if attempt >= max_attempts {
            return false;
        }
        match err {
            TransportError::Http { status, .. } => {
                (self.retry_429 && status.as_u16() == 429)
                    || (self.retry_5xx && status.is_server_error())
            }
            TransportError::Timeout | TransportError::Network(_) => self.retry_transport,
            _ => false,
        }
    }
}

pub fn backoff(base: Duration, attempt: u64) -> Duration {
    if attempt == 0 {
        return base;
    }
    let exp = 2u64.saturating_pow(attempt as u32 - 1);
    let millis = base.as_millis() as u64;
    let raw = millis.saturating_mul(exp);
    let jitter: f64 = rand::rng().random_range(0.9..1.1);
    Duration::from_millis((raw as f64 * jitter) as u64)
}

pub async fn run_with_retry<T, F, Fut>(
    policy: RetryPolicy,
    make_req: impl FnMut() -> Request,
    op: F,
) -> Result<T, TransportError>
where
    F: Fn(Request, u64) -> Fut,
    Fut: Future<Output = Result<T, TransportError>>,
{
    run_with_retry_waiting_for_network(policy, make_req, op, wait_for_network_availability).await
}

async fn run_with_retry_waiting_for_network<T, F, Fut, W, WFut>(
    policy: RetryPolicy,
    mut make_req: impl FnMut() -> Request,
    op: F,
    mut wait_for_network: W,
) -> Result<T, TransportError>
where
    F: Fn(Request, u64) -> Fut,
    Fut: Future<Output = Result<T, TransportError>>,
    W: FnMut() -> WFut,
    WFut: Future<Output = NetworkAvailabilityWait>,
{
    for attempt in 0..=policy.max_attempts {
        let req = make_req();
        match op(req, attempt).await {
            Ok(resp) => return Ok(resp),
            Err(err)
                if policy
                    .retry_on
                    .should_retry(&err, attempt, policy.max_attempts) =>
            {
                let _ = wait_for_network().await;
                sleep(backoff(policy.base_delay, attempt + 1)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Err(TransportError::RetryLimit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_availability::NetworkAvailability;
    use http::Method;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tokio::sync::Notify;

    fn retry_policy(base_delay: Duration) -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            base_delay,
            retry_on: RetryOn {
                retry_429: false,
                retry_5xx: false,
                retry_transport: true,
            },
        }
    }

    fn request() -> Request {
        Request::new(Method::GET, "https://example.test".to_string())
    }

    fn available_wait(waited: bool) -> NetworkAvailabilityWait {
        NetworkAvailabilityWait {
            availability: NetworkAvailability::Available,
            waited,
        }
    }

    #[tokio::test]
    async fn retry_waits_for_network_before_next_attempt() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_op = Arc::clone(&attempts);
        let wait_calls = Arc::new(AtomicUsize::new(0));
        let wait_calls_for_wait = Arc::clone(&wait_calls);
        let network_available = Arc::new(Notify::new());
        let network_available_for_wait = Arc::clone(&network_available);

        let task = tokio::spawn(run_with_retry_waiting_for_network(
            retry_policy(Duration::ZERO),
            request,
            move |_, _| {
                let attempts_for_op = Arc::clone(&attempts_for_op);
                async move {
                    let attempt = attempts_for_op.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(TransportError::Network("offline".to_string()))
                    } else {
                        Ok("ok")
                    }
                }
            },
            move || {
                let wait_calls_for_wait = Arc::clone(&wait_calls_for_wait);
                let network_available_for_wait = Arc::clone(&network_available_for_wait);
                async move {
                    wait_calls_for_wait.fetch_add(1, Ordering::SeqCst);
                    network_available_for_wait.notified().await;
                    available_wait(/*waited*/ true)
                }
            },
        ));

        while wait_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        network_available.notify_one();

        assert_eq!(
            task.await
                .expect("retry task should not panic")
                .expect("retry should succeed"),
            "ok"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_treats_unknown_network_availability_as_non_blocking() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_op = Arc::clone(&attempts);

        let result = run_with_retry_waiting_for_network(
            retry_policy(Duration::ZERO),
            request,
            move |_, _| {
                let attempts_for_op = Arc::clone(&attempts_for_op);
                async move {
                    let attempt = attempts_for_op.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(TransportError::Network("offline".to_string()))
                    } else {
                        Ok("ok")
                    }
                }
            },
            || async {
                NetworkAvailabilityWait {
                    availability: NetworkAvailability::Unknown,
                    waited: false,
                }
            },
        )
        .await;

        assert_eq!(result.expect("retry should succeed"), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_applies_backoff_after_network_returns() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_op = Arc::clone(&attempts);
        let network_available = Arc::new(Notify::new());
        let network_available_for_wait = Arc::clone(&network_available);

        let task = tokio::spawn(run_with_retry_waiting_for_network(
            retry_policy(Duration::from_millis(40)),
            request,
            move |_, _| {
                let attempts_for_op = Arc::clone(&attempts_for_op);
                async move {
                    let attempt = attempts_for_op.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(TransportError::Timeout)
                    } else {
                        Ok("ok")
                    }
                }
            },
            move || {
                let network_available_for_wait = Arc::clone(&network_available_for_wait);
                async move {
                    network_available_for_wait.notified().await;
                    available_wait(/*waited*/ true)
                }
            },
        ));

        while attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        network_available.notify_one();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        assert_eq!(
            task.await
                .expect("retry task should not panic")
                .expect("retry should succeed"),
            "ok"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
