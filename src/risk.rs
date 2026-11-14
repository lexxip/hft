//! Pre-trade risk controls for the ultra-low-latency path.
//!
//! [`RiskEngine`] is shared across threads through atomics only. The hot path
//! [`RiskEngine::check_order`] never allocates, never takes a lock, and never
//! calls the OS clock — the caller supplies a monotonic timestamp in nanoseconds.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use crate::order_book::Side;

/// Static risk limits loaded at start of day (or on config reload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskConfig {
    /// Hard cap on a single order quantity (lots / contracts).
    pub max_order_size: u64,
    /// Absolute open-position limit. Long and short are both bounded by this.
    pub max_position: u64,
    /// Daily loss threshold in the same fixed-point units as PnL.
    /// Trading halts when `daily_pnl <= -max_daily_loss`.
    pub max_daily_loss: i64,
    /// Maximum number of accepted orders inside one rate-limit window.
    pub max_orders_per_window: u64,
    /// Length of the rate-limit window.
    pub rate_limit_window: Duration,
}

impl RiskConfig {
    /// Builds a config and rejects values that would make the hot path unsafe
    /// or meaningless (zero-sized windows, non-positive loss caps, etc.).
    pub fn new(
        max_order_size: u64,
        max_position: u64,
        max_daily_loss: i64,
        max_orders_per_window: u64,
        rate_limit_window: Duration,
    ) -> Result<Self, ConfigError> {
        if max_order_size == 0 {
            return Err(ConfigError::MaxOrderSizeZero);
        }
        if max_position == 0 {
            return Err(ConfigError::MaxPositionZero);
        }
        if max_daily_loss <= 0 {
            return Err(ConfigError::MaxDailyLossNonPositive);
        }
        if max_orders_per_window == 0 {
            return Err(ConfigError::MaxOrdersPerWindowZero);
        }
        if rate_limit_window.is_zero() {
            return Err(ConfigError::RateLimitWindowZero);
        }

        Ok(Self {
            max_order_size,
            max_position,
            max_daily_loss,
            max_orders_per_window,
            rate_limit_window,
        })
    }

    #[inline]
    fn rate_limit_window_ns(self) -> u64 {
        // Duration::as_nanos fits in u64 for any realistic HFT window.
        self.rate_limit_window.as_nanos() as u64
    }
}

/// Configuration validation failures (construction time only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    MaxOrderSizeZero,
    MaxPositionZero,
    MaxDailyLossNonPositive,
    MaxOrdersPerWindowZero,
    RateLimitWindowZero,
}

/// Why a pre-trade check rejected an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskError {
    /// Quantity was zero.
    ZeroQuantity,
    /// Order size above [`RiskConfig::max_order_size`].
    MaxOrderSizeExceeded { size: u64, limit: u64 },
    /// Projected absolute position would exceed [`RiskConfig::max_position`].
    MaxPositionExceeded { projected: i64, limit: u64 },
    /// Daily PnL already at or below the loss limit; trading is halted.
    DailyLossLimitReached { pnl: i64, limit: i64 },
    /// Too many orders inside the current rate-limit window.
    RateLimitExceeded,
    /// Kill switch / loss halt is latched.
    TradingHalted,
}

/// Lock-free pre-trade risk engine.
///
/// Position and PnL are signed fixed-point integers (`i64`). Bids increase
/// position; asks decrease it. The rate limiter is a fixed window reset by
/// compare-and-swap on the window start timestamp.
pub struct RiskEngine {
    config: RiskConfig,
    position: AtomicI64,
    daily_pnl: AtomicI64,
    trading_halted: AtomicBool,
    window_start_ns: AtomicU64,
    orders_in_window: AtomicU64,
}

impl RiskEngine {
    /// Creates an engine at flat position, zero PnL, trading enabled.
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            position: AtomicI64::new(0),
            daily_pnl: AtomicI64::new(0),
            trading_halted: AtomicBool::new(false),
            window_start_ns: AtomicU64::new(0),
            orders_in_window: AtomicU64::new(0),
        }
    }

    /// Immutable view of the active limits.
    #[inline]
    pub fn config(&self) -> &RiskConfig {
        &self.config
    }

    /// Current signed open position.
    #[inline]
    pub fn position(&self) -> i64 {
        self.position.load(Ordering::Acquire)
    }

    /// Cumulative day PnL in fixed-point units (positive = profit).
    #[inline]
    pub fn daily_pnl(&self) -> i64 {
        self.daily_pnl.load(Ordering::Acquire)
    }

    /// Latched kill switch. Once true, [`Self::check_order`] rejects until cleared.
    #[inline]
    pub fn is_trading_halted(&self) -> bool {
        self.trading_halted.load(Ordering::Acquire)
    }

    /// Pre-trade gate. Must complete in microseconds with no heap traffic.
    ///
    /// `now_ns` is a monotonic nanosecond clock owned by the caller so this
    /// method never issues a syscall. On success the order consumes one slot
    /// in the rate-limit window.
    #[inline]
    pub fn check_order(&self, side: Side, quantity: u64, now_ns: u64) -> Result<(), RiskError> {
        if self.trading_halted.load(Ordering::Acquire) {
            return Err(RiskError::TradingHalted);
        }

        // Re-check PnL under the halt latch: a concurrent mark-to-market may
        // have crossed the loss limit without yet flipping the bool.
        let pnl = self.daily_pnl.load(Ordering::Acquire);
        if pnl <= -self.config.max_daily_loss {
            self.trading_halted.store(true, Ordering::Release);
            return Err(RiskError::DailyLossLimitReached {
                pnl,
                limit: self.config.max_daily_loss,
            });
        }

        if quantity == 0 {
            return Err(RiskError::ZeroQuantity);
        }

        if quantity > self.config.max_order_size {
            return Err(RiskError::MaxOrderSizeExceeded {
                size: quantity,
                limit: self.config.max_order_size,
            });
        }

        let position = self.position.load(Ordering::Acquire);
        let qty = quantity as i64;
        let projected = match side {
            Side::Bid => position.saturating_add(qty),
            Side::Ask => position.saturating_sub(qty),
        };

        if projected.unsigned_abs() > self.config.max_position {
            return Err(RiskError::MaxPositionExceeded {
                projected,
                limit: self.config.max_position,
            });
        }

        self.consume_rate_limit_slot(now_ns)?;
        Ok(())
    }

    /// Applies a fill to the open position after the exchange acknowledges it.
    #[inline]
    pub fn record_fill(&self, side: Side, quantity: u64) {
        let qty = quantity as i64;
        match side {
            Side::Bid => {
                self.position.fetch_add(qty, Ordering::AcqRel);
            }
            Side::Ask => {
                self.position.fetch_sub(qty, Ordering::AcqRel);
            }
        }
    }

    /// Adds a realized / marked PnL delta. Crosses the daily loss limit latch
    /// the halt so subsequent [`Self::check_order`] calls fail fast.
    #[inline]
    pub fn record_pnl(&self, delta: i64) {
        let pnl = self.daily_pnl.fetch_add(delta, Ordering::AcqRel) + delta;
        if pnl <= -self.config.max_daily_loss {
            self.trading_halted.store(true, Ordering::Release);
        }
    }

    /// Manual kill switch (ops / kill-switch wire).
    #[inline]
    pub fn halt_trading(&self) {
        self.trading_halted.store(true, Ordering::Release);
    }

    /// Clears the halt latch. Does not reset position or PnL.
    #[inline]
    pub fn resume_trading(&self) {
        self.trading_halted.store(false, Ordering::Release);
    }

    /// Start-of-day reset: flat PnL, open the gate, empty the rate window.
    /// Position is left untouched — flatten inventory separately if required.
    pub fn reset_daily(&self) {
        self.daily_pnl.store(0, Ordering::Release);
        self.trading_halted.store(false, Ordering::Release);
        self.orders_in_window.store(0, Ordering::Release);
        self.window_start_ns.store(0, Ordering::Release);
    }

    /// Fixed-window rate limiter. Resets the window with CAS when it expires;
    /// otherwise increments the counter and rolls back on overflow.
    #[inline]
    fn consume_rate_limit_slot(&self, now_ns: u64) -> Result<(), RiskError> {
        let window_ns = self.config.rate_limit_window_ns();

        loop {
            let start = self.window_start_ns.load(Ordering::Acquire);

            // First order ever, or the window has expired → try to open a new one.
            if start == 0 || now_ns.saturating_sub(start) >= window_ns {
                match self.window_start_ns.compare_exchange(
                    start,
                    now_ns,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.orders_in_window.store(1, Ordering::Release);
                        return Ok(());
                    }
                    Err(_) => {
                        // Another thread won the reset; retry against the new window.
                        continue;
                    }
                }
            }

            let previous = self.orders_in_window.fetch_add(1, Ordering::AcqRel);
            if previous >= self.config.max_orders_per_window {
                self.orders_in_window.fetch_sub(1, Ordering::AcqRel);
                return Err(RiskError::RateLimitExceeded);
            }

            return Ok(());
        }
    }
}

impl Default for RiskEngine {
    fn default() -> Self {
        // Sensible demo defaults; production must load RiskConfig from TOML.
        let config = RiskConfig::new(100, 1_000, 10_000, 100, Duration::from_millis(1))
            .expect("default RiskConfig is valid");
        Self::new(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RiskConfig {
        RiskConfig::new(
            10,                         // max order size
            50,                         // max absolute position
            1_000,                      // max daily loss
            3,                          // max orders / window
            Duration::from_millis(100), // window
        )
        .unwrap()
    }

    #[test]
    fn rejects_zero_and_oversized_orders() {
        let engine = RiskEngine::new(test_config());

        assert_eq!(engine.check_order(Side::Bid, 0, 1), Err(RiskError::ZeroQuantity));
        assert_eq!(
            engine.check_order(Side::Bid, 11, 1),
            Err(RiskError::MaxOrderSizeExceeded {
                size: 11,
                limit: 10
            })
        );
        assert!(engine.check_order(Side::Bid, 10, 1).is_ok());
    }

    #[test]
    fn enforces_max_position_on_both_sides() {
        let engine = RiskEngine::new(test_config());

        // Build a long of 45 via fills (check_order does not mutate position).
        engine.record_fill(Side::Bid, 45);
        assert_eq!(engine.position(), 45);

        // +10 → 55 > 50
        assert_eq!(
            engine.check_order(Side::Bid, 10, 1),
            Err(RiskError::MaxPositionExceeded {
                projected: 55,
                limit: 50
            })
        );

        // Reducing the long is fine.
        assert!(engine.check_order(Side::Ask, 10, 2).is_ok());

        // Flip to a short of -45 and block further selling past the limit.
        engine.record_fill(Side::Ask, 90); // 45 - 90 = -45
        assert_eq!(engine.position(), -45);
        assert_eq!(
            engine.check_order(Side::Ask, 10, 3),
            Err(RiskError::MaxPositionExceeded {
                projected: -55,
                limit: 50
            })
        );
        assert!(engine.check_order(Side::Bid, 10, 4).is_ok());
    }

    #[test]
    fn daily_loss_latches_trading_halt() {
        let engine = RiskEngine::new(test_config());

        assert!(engine.check_order(Side::Bid, 1, 1).is_ok());
        assert!(!engine.is_trading_halted());

        // Cross the loss limit: pnl = -1000.
        engine.record_pnl(-1_000);
        assert!(engine.is_trading_halted());
        assert_eq!(
            engine.check_order(Side::Bid, 1, 2),
            Err(RiskError::TradingHalted)
        );

        // Resume without resetting PnL still fails the PnL gate and re-latches.
        engine.resume_trading();
        match engine.check_order(Side::Bid, 1, 3) {
            Err(RiskError::DailyLossLimitReached { pnl, limit }) => {
                assert_eq!(pnl, -1_000);
                assert_eq!(limit, 1_000);
            }
            other => panic!("expected DailyLossLimitReached, got {other:?}"),
        }
        assert!(engine.is_trading_halted());

        // Start of day clears PnL and opens the gate.
        engine.reset_daily();
        assert_eq!(engine.daily_pnl(), 0);
        assert!(!engine.is_trading_halted());
        assert!(engine.check_order(Side::Bid, 1, 4).is_ok());
    }

    #[test]
    fn rate_limit_blocks_burst_then_opens_on_new_window() {
        let engine = RiskEngine::new(test_config());
        let t0 = 1_000_000u64; // 1 ms in ns

        assert!(engine.check_order(Side::Bid, 1, t0).is_ok());
        assert!(engine.check_order(Side::Bid, 1, t0 + 1).is_ok());
        assert!(engine.check_order(Side::Bid, 1, t0 + 2).is_ok());
        assert_eq!(
            engine.check_order(Side::Bid, 1, t0 + 3),
            Err(RiskError::RateLimitExceeded)
        );

        // Still inside the 100 ms window.
        assert_eq!(
            engine.check_order(Side::Bid, 1, t0 + 99_999_999),
            Err(RiskError::RateLimitExceeded)
        );

        // Window rolls; a fresh burst is allowed.
        let t1 = t0 + 100_000_000;
        assert!(engine.check_order(Side::Bid, 1, t1).is_ok());
        assert!(engine.check_order(Side::Bid, 1, t1 + 1).is_ok());
        assert!(engine.check_order(Side::Bid, 1, t1 + 2).is_ok());
        assert_eq!(
            engine.check_order(Side::Bid, 1, t1 + 3),
            Err(RiskError::RateLimitExceeded)
        );
    }

    #[test]
    fn manual_halt_blocks_until_resumed() {
        let engine = RiskEngine::new(test_config());
        engine.halt_trading();
        assert_eq!(
            engine.check_order(Side::Ask, 1, 1),
            Err(RiskError::TradingHalted)
        );
        engine.resume_trading();
        assert!(engine.check_order(Side::Ask, 1, 2).is_ok());
    }

    #[test]
    fn config_rejects_invalid_limits() {
        assert_eq!(
            RiskConfig::new(0, 1, 1, 1, Duration::from_millis(1)),
            Err(ConfigError::MaxOrderSizeZero)
        );
        assert_eq!(
            RiskConfig::new(1, 1, 0, 1, Duration::from_millis(1)),
            Err(ConfigError::MaxDailyLossNonPositive)
        );
        assert_eq!(
            RiskConfig::new(1, 1, 1, 1, Duration::ZERO),
            Err(ConfigError::RateLimitWindowZero)
        );
    }

    #[test]
    fn check_order_does_not_mutate_position() {
        let engine = RiskEngine::new(test_config());
        engine.check_order(Side::Bid, 5, 1).unwrap();
        assert_eq!(engine.position(), 0);
        engine.record_fill(Side::Bid, 5);
        assert_eq!(engine.position(), 5);
    }
}
