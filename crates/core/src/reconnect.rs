use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Stop retrying after this many attempts. `None` = no limit.
    pub max_retries:  Option<u32>,
    /// Stop retrying after this wall-clock duration. `None` = no limit.
    pub max_duration: Option<Duration>,
    pub initial_delay: Duration,
    pub max_delay:     Duration,
    pub jitter:        bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_retries:   None,
            max_duration:  Some(Duration::from_secs(3600)),
            initial_delay: Duration::from_secs(1),
            max_delay:     Duration::from_secs(60),
            jitter:        true,
        }
    }
}

impl ReconnectPolicy {
    /// Computes the sleep duration for the given attempt number (0-based).
    pub fn next_delay(&self, attempt: u32) -> Duration {
        let base_secs = self.initial_delay.as_secs_f64()
            * 2_f64.powi(attempt as i32);
        let capped = base_secs.min(self.max_delay.as_secs_f64());

        let final_secs = if self.jitter {
            use rand::Rng;
            let factor: f64 = rand::thread_rng().gen_range(0.5..=1.0);
            capped * factor
        } else {
            capped
        };

        Duration::from_secs_f64(final_secs.max(0.1))
    }

    /// Returns `true` if the retry limit has been reached by either criterion.
    ///
    /// `attempt` is the number of failed attempts so far (0-based before first retry).
    /// `elapsed` is the wall-clock time since the first failure.
    pub fn is_exceeded(&self, attempt: u32, elapsed: Duration) -> bool {
        if let Some(max) = self.max_retries {
            if attempt >= max {
                return true;
            }
        }
        if let Some(max_dur) = self.max_duration {
            if elapsed >= max_dur {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_and_caps() {
        let policy = ReconnectPolicy {
            max_retries:   Some(10),
            max_duration:  None,
            initial_delay: Duration::from_secs(1),
            max_delay:     Duration::from_secs(60),
            jitter:        false,
        };

        assert_eq!(policy.next_delay(0), Duration::from_secs(1));
        assert_eq!(policy.next_delay(1), Duration::from_secs(2));
        assert_eq!(policy.next_delay(2), Duration::from_secs(4));
        assert_eq!(policy.next_delay(10), Duration::from_secs(60));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let policy = ReconnectPolicy {
            max_retries:   None,
            max_duration:  None,
            initial_delay: Duration::from_secs(2),
            max_delay:     Duration::from_secs(60),
            jitter:        true,
        };

        for _ in 0..100 {
            let d = policy.next_delay(3); // base = 16s
            assert!(d >= Duration::from_secs(7));
            assert!(d <= Duration::from_secs(17));
        }
    }

    #[test]
    fn is_exceeded_by_count() {
        let policy = ReconnectPolicy {
            max_retries:   Some(3),
            max_duration:  None,
            ..Default::default()
        };
        assert!(!policy.is_exceeded(2, Duration::from_secs(0)));
        assert!(policy.is_exceeded(3, Duration::from_secs(0)));
    }

    #[test]
    fn is_exceeded_by_duration() {
        let policy = ReconnectPolicy {
            max_retries:   None,
            max_duration:  Some(Duration::from_secs(60)),
            ..Default::default()
        };
        assert!(!policy.is_exceeded(99, Duration::from_secs(59)));
        assert!(policy.is_exceeded(99, Duration::from_secs(60)));
    }
}
