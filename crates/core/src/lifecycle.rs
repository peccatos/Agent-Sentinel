//! Session lifecycle as a finite state machine. The reliability
//! analyzer (Phase 3) checks real event streams against this model.

use crate::event::EventKind;

/// Session states derived from the event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Before `SessionStarted` was seen.
    NotStarted,
    Running,
    Completed,
    Failed,
}

/// Expected transitions: which event kinds may legally appear while
/// the session is in each state.
impl SessionState {
    pub fn on_event(self, kind: EventKind) -> Transition {
        use EventKind as E;
        use SessionState as S;
        match (self, kind) {
            (S::NotStarted, E::SessionStarted) => Transition::Legal(S::Running),
            (S::NotStarted, _) => Transition::MissingSessionStart,
            (S::Running, E::SessionCompleted) => Transition::Legal(S::Completed),
            (S::Running, E::SessionFailed) => Transition::Legal(S::Failed),
            (S::Running, _) => Transition::Legal(S::Running),
            (S::Completed, E::SessionStarted) => Transition::UnexpectedRestart,
            (S::Failed, E::SessionStarted) => Transition::UnexpectedRestart,
            (S::Completed, _) | (S::Failed, _) => Transition::EventsAfterClose,
        }
    }
}

/// Result of applying an event to a session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Expected transition; carries the new state.
    Legal(SessionState),
    /// An event arrived before `SessionStarted` (missing opening event).
    MissingSessionStart,
    /// `SessionStarted` for an already-closed session.
    UnexpectedRestart,
    /// Any event after the session closed.
    EventsAfterClose,
}

/// Single-pass session tracker. Feed events in order; collect anomalies.
#[derive(Debug)]
pub struct SessionTracker {
    state: SessionState,
    started_seen: bool,
    closed: bool,
}

impl Default for SessionTracker {
    fn default() -> Self {
        Self {
            state: SessionState::NotStarted,
            started_seen: false,
            closed: false,
        }
    }
}

impl SessionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Applies an event and returns the transition verdict.
    pub fn observe(&mut self, kind: EventKind) -> Transition {
        let t = self.state.on_event(kind);
        match t {
            Transition::Legal(next) => {
                self.state = next;
                if matches!(kind, EventKind::SessionStarted) {
                    self.started_seen = true;
                }
                if matches!(
                    kind,
                    EventKind::SessionCompleted | EventKind::SessionFailed
                ) {
                    self.closed = true;
                }
            }
            _ => {}
        }
        t
    }

    pub fn is_open(&self) -> bool {
        self.started_seen && !self.closed
    }

    /// INV-4: every session must eventually be closed by either
    /// `SessionCompleted` or `SessionFailed`.
    pub fn closing_event_seen(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind::*;

    #[test]
    fn happy_path() {
        let mut t = SessionTracker::new();
        assert_eq!(t.observe(SessionStarted), Transition::Legal(SessionState::Running));
        assert_eq!(t.observe(UserPrompt), Transition::Legal(SessionState::Running));
        assert_eq!(t.observe(ToolCall), Transition::Legal(SessionState::Running));
        assert_eq!(
            t.observe(SessionCompleted),
            Transition::Legal(SessionState::Completed)
        );
        assert!(!t.is_open());
        assert!(t.closing_event_seen());
    }

    #[test]
    fn missing_start_is_flagged() {
        let mut t = SessionTracker::new();
        assert_eq!(t.observe(ToolCall), Transition::MissingSessionStart);
    }

    #[test]
    fn events_after_close_are_flagged() {
        let mut t = SessionTracker::new();
        t.observe(SessionStarted);
        t.observe(SessionFailed);
        assert_eq!(t.observe(ToolCall), Transition::EventsAfterClose);
    }

    #[test]
    fn restart_is_flagged() {
        let mut t = SessionTracker::new();
        t.observe(SessionStarted);
        t.observe(SessionCompleted);
        assert_eq!(
            t.observe(SessionStarted),
            Transition::UnexpectedRestart
        );
    }
}
