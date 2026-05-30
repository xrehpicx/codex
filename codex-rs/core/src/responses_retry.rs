//! Shared retry and transport fallback decisions for Responses requests.

use std::time::Duration;

use crate::client::ModelClientSession;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::util::backoff;
use codex_api::NetworkAvailability;
use codex_api::current_network_availability;
use codex_api::wait_for_network_availability;
use codex_protocol::error::CodexErr;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::StreamErrorEvent;
use codex_protocol::protocol::WarningEvent;
use tracing::warn;

const WAITING_FOR_NETWORK_MESSAGE: &str = "Waiting for network connection...";

#[derive(Debug, Clone, Copy)]
pub(crate) enum ResponsesStreamRequest {
    Sampling,
    RemoteCompactionV2,
}

#[derive(Debug, PartialEq, Eq)]
struct RetryAttempt {
    retry_count: u64,
    report_error: bool,
}

/// Handles a retryable stream error and returns `Ok(())` when the caller should
/// retry the request loop.
pub(crate) async fn handle_retryable_response_stream_error(
    retries: &mut u64,
    max_retries: u64,
    err: CodexErr,
    client_session: &mut ModelClientSession,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
) -> Result<(), CodexErr> {
    wait_for_network_before_stream_retry(sess, turn_context, &err).await;

    if *retries >= max_retries
        && client_session.try_switch_fallback_transport(
            &turn_context.session_telemetry,
            &turn_context.model_info,
        )
    {
        sess.send_event(
            turn_context,
            EventMsg::Warning(WarningEvent {
                message: format!("Falling back from WebSockets to HTTPS transport. {err:#}"),
            }),
        )
        .await;
        *retries = 0;
        return Ok(());
    }

    if let Some(RetryAttempt {
        retry_count,
        report_error,
    }) = next_retry_attempt(
        retries,
        max_retries,
        sess.services.model_client.responses_websocket_enabled(),
    ) {
        let delay = match &err {
            CodexErr::Stream(_, requested_delay) => {
                requested_delay.unwrap_or_else(|| backoff(retry_count))
            }
            _ => backoff(retry_count),
        };
        log_retry(request, turn_context, &err, retry_count, max_retries, delay);

        // In release builds, hide the first websocket retry notification to reduce noisy
        // transient reconnect messages. In debug builds, keep full visibility for diagnosis.
        if report_error {
            // Surface retry information to any UI/front-end so the user understands what is
            // happening instead of staring at a seemingly frozen screen.
            sess.notify_stream_error(
                turn_context,
                format!("Reconnecting... {retry_count}/{max_retries}"),
                err,
            )
            .await;
        }
        tokio::time::sleep(delay).await;
        return Ok(());
    }

    Err(err)
}

fn next_retry_attempt(
    retries: &mut u64,
    max_retries: u64,
    responses_websocket_enabled: bool,
) -> Option<RetryAttempt> {
    if *retries >= max_retries {
        return None;
    }

    *retries += 1;
    let retry_count = *retries;
    Some(RetryAttempt {
        retry_count,
        report_error: retry_count > 1 || cfg!(debug_assertions) || !responses_websocket_enabled,
    })
}

pub(crate) async fn wait_for_network_before_stream_retry(
    sess: &Session,
    turn_context: &TurnContext,
    err: &CodexErr,
) {
    if current_network_availability() != NetworkAvailability::Unavailable {
        return;
    }

    notify_waiting_for_network(sess, turn_context, err).await;
    let network_wait = wait_for_network_availability().await;
    if network_wait.waited {
        warn!("local network was unavailable; retrying response stream after network returned");
    }
}

async fn notify_waiting_for_network(sess: &Session, turn_context: &TurnContext, err: &CodexErr) {
    sess.send_event(turn_context, waiting_for_network_event(err))
        .await;
}

fn waiting_for_network_event(err: &CodexErr) -> EventMsg {
    EventMsg::StreamError(StreamErrorEvent {
        message: WAITING_FOR_NETWORK_MESSAGE.to_string(),
        codex_error_info: Some(CodexErrorInfo::ResponseStreamDisconnected {
            http_status_code: err.http_status_code_value(),
        }),
        additional_details: Some(err.to_string()),
    })
}

fn log_retry(
    request: ResponsesStreamRequest,
    turn_context: &TurnContext,
    err: &CodexErr,
    retries: u64,
    max_retries: u64,
    delay: Duration,
) {
    match request {
        ResponsesStreamRequest::Sampling => {
            warn!(
                "stream disconnected - retrying sampling request ({retries}/{max_retries} in {delay:?})...",
            );
        }
        ResponsesStreamRequest::RemoteCompactionV2 => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                compact_error = %err,
                "remote compaction v2 stream failed; retrying request after delay"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::protocol::StreamErrorEvent;
    use pretty_assertions::assert_eq;

    #[test]
    fn next_retry_attempt_increments_retry_counter() {
        let mut retries = 0;

        assert_eq!(
            next_retry_attempt(&mut retries, 2, /*responses_websocket_enabled*/ false),
            Some(RetryAttempt {
                retry_count: 1,
                report_error: true,
            })
        );
        assert_eq!(retries, 1);

        assert_eq!(
            next_retry_attempt(&mut retries, 2, /*responses_websocket_enabled*/ true),
            Some(RetryAttempt {
                retry_count: 2,
                report_error: true,
            })
        );
        assert_eq!(retries, 2);

        assert_eq!(
            next_retry_attempt(&mut retries, 2, /*responses_websocket_enabled*/ true),
            None
        );
        assert_eq!(retries, 2);
    }

    #[test]
    fn next_retry_attempt_hides_first_websocket_retry_in_release() {
        let mut retries = 0;

        assert_eq!(
            next_retry_attempt(&mut retries, 2, /*responses_websocket_enabled*/ true),
            Some(RetryAttempt {
                retry_count: 1,
                report_error: cfg!(debug_assertions),
            })
        );
    }

    #[test]
    fn waiting_for_network_event_uses_retry_status_message() {
        let err = CodexErr::Stream("network error".to_string(), None);

        let EventMsg::StreamError(StreamErrorEvent {
            message,
            additional_details,
            codex_error_info,
        }) = waiting_for_network_event(&err)
        else {
            panic!("expected stream error event");
        };

        assert_eq!(message, WAITING_FOR_NETWORK_MESSAGE);
        assert_eq!(
            additional_details,
            Some("stream disconnected before completion: network error".to_string())
        );
        assert_eq!(
            codex_error_info,
            Some(CodexErrorInfo::ResponseStreamDisconnected {
                http_status_code: None
            })
        );
    }
}
