use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use crate::order_book::{OrderBook, Side};
use crate::risk::RiskEngine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Signal {
    pub side: Side,
    pub price: u64,
    pub quantity: u64,
}

pub trait Strategy {
    fn name(&self) -> &'static str;
    fn evaluate(&mut self, book: &OrderBook, risk: &RiskEngine, now_ns: u64) -> Option<Signal>;
}

pub struct AdvancedScalper {
    order_size: u64,
    max_inventory: i64,
    max_offset_bps: u64,
    tick_size: u64,
    
    // Состояние позиции: (Side, EntryPrice, BestPriceSeen)
    active_position: Option<(Side, u64, u64)>,
    trailing_stop_ticks: u64,
    target_profit_ticks: u64,
    
    // Предвыделенные буферы для стакана (Zero-Allocation в горячем пути)
    bids_buf: Vec<(u64, u64)>,
    asks_buf: Vec<(u64, u64)>,
    
    // Буферизированный логгер для быстрой записи без системных вызовов на каждый тик
    csv_writer: Option<BufWriter<File>>,
}

impl AdvancedScalper {
    pub fn new(order_size: u64, max_inventory: i64, max_offset_bps: u64, tick_size: u64) -> Self {
        let filename = "trades_log.csv".to_string();
        let file_exists = Path::new(&filename).exists();
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filename);

        let csv_writer = match file {
            Ok(f) => {
                let mut writer = BufWriter::new(f);
                if !file_exists {
                    let _ = writeln!(writer, "timestamp_ns,side,entry_price,exit_price,quantity,pnl");
                }
                Some(writer)
            }
            Err(e) => {
                log::warn!("failed to open {}: {}", filename, e);
                None
            }
        };

        Self {
            order_size,
            max_inventory,
            max_offset_bps,
            tick_size,
            active_position: None,
            trailing_stop_ticks: 4,
            target_profit_ticks: 8,
            bids_buf: vec![(0, 0); 50],
            asks_buf: vec![(0, 0); 50],
            csv_writer,
        }
    }

    fn log_trade(&mut self, timestamp_ns: u64, side: Side, entry: u64, exit: u64, qty: u64, pnl: i64) {
        if let Some(writer) = &mut self.csv_writer {
            let side_str = match side { Side::Bid => "BUY", Side::Ask => "SELL" };
            let _ = writeln!(writer, "{},{},{},{},{},{}", timestamp_ns, side_str, entry, exit, qty, pnl);
            // let _ = writer.flush();
        }
    }
}

impl Strategy for AdvancedScalper {
    fn name(&self) -> &'static str {
        "AdvancedWindowScalper"
    }

    fn evaluate(&mut self, book: &OrderBook, risk: &RiskEngine, now_ns: u64) -> Option<Signal> {
        let (best_bid, best_ask) = match (book.best_bid(), book.best_ask()) {
            (Some(b), Some(a)) => (b.0, a.0),
            _ => return None,
        };
        if best_ask < best_bid {
            return None;
        }

        let mid_price = best_bid + (best_ask - best_bid) / 2;
        if mid_price == 0 {
            return None;
        }

        let current_position = risk.position();

        // 1. Управление активной позицией (Trailing Stop & Take Profit)
        if let Some((side, entry_price, ref mut best_price)) = self.active_position {
            match side {
                Side::Bid => {
                    if best_bid > *best_price {
                        *best_price = best_bid;
                    }
                    let stop_price = best_price.saturating_sub(self.trailing_stop_ticks * self.tick_size);
                    let target_price = entry_price.saturating_add(self.target_profit_ticks * self.tick_size);

                    if best_bid <= stop_price || best_bid >= target_price {
                        let exit_price = best_bid;
                        let pnl = ((exit_price as i64) - (entry_price as i64)) * (self.order_size as i64);
                        self.log_trade(now_ns, Side::Ask, entry_price, exit_price, self.order_size, pnl);
                        risk.record_pnl(pnl);
                        // TODO: move record_fill to execution report ACK from exchange
                        risk.record_fill(Side::Ask, self.order_size);
                        self.active_position = None;
                    }
                }
                Side::Ask => {
                    if best_ask < *best_price {
                        *best_price = best_ask;
                    }
                    let stop_price = best_price.saturating_add(self.trailing_stop_ticks * self.tick_size);
                    let target_price = entry_price.saturating_sub(self.target_profit_ticks * self.tick_size);

                    if best_ask >= stop_price || best_ask <= target_price {
                        let exit_price = best_ask;
                        let pnl = ((entry_price as i64) - (exit_price as i64)) * (self.order_size as i64);
                        self.log_trade(now_ns, Side::Bid, entry_price, exit_price, self.order_size, pnl);
                        risk.record_pnl(pnl);
                        // TODO: move record_fill to execution report ACK from exchange
                        risk.record_fill(Side::Bid, self.order_size);
                        self.active_position = None;
                    }
                }
            }
            return None;
        }

        // 2. Сбор объемов в динамическом окне (Zero-Allocation с использованием предвыделенных буферов)
        let (b_len, a_len) = book.copy_top_levels(&mut self.bids_buf, &mut self.asks_buf);

        let mut total_bid_vol: u64 = 0;
        let mut total_ask_vol: u64 = 0;

        for &(p, v) in &self.bids_buf[..b_len] {
            let offset_bps = ((mid_price.saturating_sub(p)) * 10_000) / mid_price;
            if offset_bps <= self.max_offset_bps {
                total_bid_vol += v;
            }
        }

        for &(p, v) in &self.asks_buf[..a_len] {
            let offset_bps = ((p.saturating_sub(mid_price)) * 10_000) / mid_price;
            if offset_bps <= self.max_offset_bps {
                total_ask_vol += v;
            }
        }

        let can_buy = current_position < self.max_inventory;
        let can_sell = current_position > -self.max_inventory;

        // 3. Генерация сигнала по дисбалансу объемов в окне
        if total_bid_vol > total_ask_vol && can_buy {
            if risk.check_order(Side::Bid, self.order_size, now_ns).is_ok() {
                self.active_position = Some((Side::Bid, best_ask, best_ask));
                // TODO: move record_fill to execution report ACK from exchange
                risk.record_fill(Side::Bid, self.order_size);
                return Some(Signal {
                    side: Side::Bid,
                    price: best_ask,
                    quantity: self.order_size,
                });
            }
        } else if total_ask_vol > total_bid_vol && can_sell {
            if risk.check_order(Side::Ask, self.order_size, now_ns).is_ok() {
                self.active_position = Some((Side::Ask, best_bid, best_bid));
                // TODO: move record_fill to execution report ACK from exchange
                risk.record_fill(Side::Ask, self.order_size);
                return Some(Signal {
                    side: Side::Ask,
                    price: best_bid,
                    quantity: self.order_size,
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::RiskConfig;
    use std::time::Duration;

    #[test]
    fn test_advanced_scalper_strategy() {
        let mut book = OrderBook::new();
        let config = RiskConfig::new(10, 100, 1000, 50, Duration::from_secs(1)).unwrap();
        let risk = RiskEngine::new(config);
        let mut scalper = AdvancedScalper::new(2, 10, 10, 1);

        // Явный дисбаланс бидов в пределах окна
        book.update_level(Side::Bid, 10000, 1000);
        book.update_level(Side::Ask, 10002, 10);

        let signal = scalper.evaluate(&book, &risk, 1000);
        assert!(signal.is_some());
        let sig = signal.unwrap();
        assert_eq!(sig.side, Side::Bid);
    }
}