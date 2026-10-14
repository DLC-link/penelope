//! Failure diagnostics for polled Then probes.
//!
//! A probe returns `None` to mean "not observable yet, keep polling". That
//! makes every failure — a restart window, a 500, a typo in the probe's own
//! request — look identical to the [`Scenario`](crate::Scenario) runner,
//! which then reports nothing but "timed out after 60s".
//!
//! [`ProbeDiag`] is the side channel that closes the gap: probes record what
//! went wrong, the runner reads it back to annotate the timeout and to end the
//! step early when the request itself is the problem.
//!
//! The probe classifies each failure, because only the probe knows its
//! protocol. An HTTP probe might call a 503 transient and a 400 fatal.

use std::sync::{Arc, Mutex, MutexGuard};

use tracing::{debug, warn};

/// Consecutive identical [`Class::Fatal`] failures that end a polled step early
/// instead of letting it run to its deadline.
pub const FATAL_STREAK: u32 = 3;

/// Whether a probe failure can still resolve if the probe is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The SUT may still get there: connection refused during a restart, a
    /// 5xx, a token blip, or a handler answering "that resource does not exist
    /// yet" for something a later step creates.
    Transient,
    /// The request will stay wrong however long it is retried: no such route,
    /// a malformed query, a success body the probe cannot parse.
    Fatal,
}

#[derive(Debug, Default)]
struct Diag {
    last_error: Option<String>,
    key: Option<String>,
    class: Option<Class>,
    repeats: u32,
}

/// Shared record of the most recent probe failure, cleared by the runner at
/// the start of every polled step.
///
/// Clones share one record, so a probe can move a clone into its future.
#[derive(Debug, Clone, Default)]
pub struct ProbeDiag(Arc<Mutex<Diag>>);

impl ProbeDiag {
    fn guard(&self) -> MutexGuard<'_, Diag> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record a failure against `key` (a request identity, e.g. `GET /orders`).
    /// Repeats of the same key are counted; the first is logged at `warn` and
    /// the rest at `debug` so a 60s poll cannot bury the run's real warnings.
    pub fn record(&self, key: &str, class: Class, message: String) {
        let mut d = self.guard();
        if d.key.as_deref() == Some(key) && d.class == Some(class) {
            d.repeats += 1;
        } else {
            d.key = Some(key.to_string());
            d.class = Some(class);
            d.repeats = 1;
        }
        if d.repeats == 1 {
            warn!("probe failure ({class:?}): {message}");
        } else {
            debug!("probe failure ({class:?}, x{}): {message}", d.repeats);
        }
        d.last_error = Some(message);
    }

    /// Keep polling on `Err`, but record why first. For probe reads that
    /// cannot be classified — a database count, a multi-call query — so the
    /// failure is recorded as transient and never ends the step early.
    pub fn ok<T>(&self, key: &str, result: anyhow::Result<T>) -> Option<T> {
        match result {
            Ok(v) => {
                self.record_ok(key);
                Some(v)
            }
            Err(e) => {
                self.record(key, Class::Transient, format!("{key}: {e:#}"));
                None
            }
        }
    }

    /// Note that `key` succeeded, ending any streak it had accumulated.
    pub fn record_ok(&self, key: &str) {
        let mut d = self.guard();
        if d.key.as_deref() == Some(key) {
            *d = Diag::default();
        }
    }

    /// The failure a polled step should stop on: the same fatal request
    /// repeated [`FATAL_STREAK`] times.
    pub fn fatal(&self) -> Option<String> {
        let d = self.guard();
        (d.class == Some(Class::Fatal) && d.repeats >= FATAL_STREAK)
            .then(|| d.last_error.clone())
            .flatten()
    }

    /// The message of the most recently recorded failure, if any.
    pub fn last_error(&self) -> Option<String> {
        self.guard().last_error.clone()
    }

    /// Forget every recorded failure.
    pub fn reset(&self) {
        *self.guard() = Diag::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_reported_only_after_the_streak() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK - 1 {
            diag.record("GET /typo", Class::Fatal, "404".to_string());
            assert_eq!(diag.fatal(), None);
        }
        diag.record("GET /typo", Class::Fatal, "404".to_string());
        assert_eq!(diag.fatal().as_deref(), Some("404"));
    }

    #[test]
    fn a_different_request_restarts_the_streak() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK {
            diag.record("GET /a", Class::Fatal, "404 a".to_string());
        }
        diag.record("GET /b", Class::Fatal, "404 b".to_string());
        assert_eq!(diag.fatal(), None);
        assert_eq!(diag.last_error().as_deref(), Some("404 b"));
    }

    #[test]
    fn a_class_change_on_the_same_request_restarts_the_streak() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK - 1 {
            diag.record("GET /a", Class::Fatal, "404".to_string());
        }
        diag.record("GET /a", Class::Transient, "503".to_string());
        diag.record("GET /a", Class::Fatal, "404".to_string());
        assert_eq!(diag.fatal(), None);
    }

    #[test]
    fn transient_repeats_never_become_fatal() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK * 3 {
            diag.record("GET /x", Class::Transient, "connection refused".to_string());
        }
        assert_eq!(diag.fatal(), None);
        assert_eq!(diag.last_error().as_deref(), Some("connection refused"));
    }

    #[test]
    fn success_clears_its_own_streak_only() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK {
            diag.record("GET /a", Class::Fatal, "404 a".to_string());
        }
        diag.record_ok("GET /b");
        assert_eq!(diag.fatal().as_deref(), Some("404 a"));
        diag.record_ok("GET /a");
        assert_eq!(diag.fatal(), None);
        assert_eq!(diag.last_error(), None);
    }

    #[test]
    fn ok_records_an_error_as_transient_and_passes_a_value_through() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK * 2 {
            let read: anyhow::Result<u32> = Err(anyhow::anyhow!("db locked"));
            assert_eq!(diag.ok("COUNT orders", read), None);
        }
        assert_eq!(diag.fatal(), None);
        assert_eq!(
            diag.last_error().as_deref(),
            Some("COUNT orders: db locked")
        );

        assert_eq!(diag.ok("COUNT orders", Ok(7)), Some(7));
        assert_eq!(diag.last_error(), None);
    }

    #[test]
    fn clones_share_one_record() {
        let diag = ProbeDiag::default();
        let clone = diag.clone();
        clone.record("GET /a", Class::Transient, "503".to_string());
        assert_eq!(diag.last_error().as_deref(), Some("503"));
    }

    #[test]
    fn reset_clears_everything() {
        let diag = ProbeDiag::default();
        for _ in 0..FATAL_STREAK {
            diag.record("GET /a", Class::Fatal, "404 a".to_string());
        }
        diag.reset();
        assert_eq!(diag.fatal(), None);
        assert_eq!(diag.last_error(), None);
    }
}
