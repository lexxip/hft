use std::fs::OpenOptions;
use std::io::Write;
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
    max_offset_bps: u64, // Динамическое ценовое окно в базисных пунктах (например, 10 = 0.1% от мид-цены)
    tick_size: u64,
    
    // Состояние открытой позиции для Трейлинг-стопа: (Side, EntryPrice, BestPriceSeen)
    active_position: Option<(Side, u64, u64)>,
    trailing_stop_ticks: u64,
    target_profit_ticks: u64,
    
    csv_filename: String,
}

impl AdvancedScalper {
    pub fn new(order_size: u64, max_inventory: i64, max_offset_bps: u64, tick_size: u64) -> Self {
        let filename = "trades_log.csv".to_string();
        
        // Создаем CSV-файл с заголовками, если он еще не существует
        if !Path::new(&filename).exists() {
            if let Ok(mut file) = OpenOptions::new().create(true).write(true).open(&filename) {
                let _ = writeln!(file, "timestamp_ns,side,entry_price,exit_price,quantity,pnl");
            }
        }

        Self {
            order_size,
            max_inventory,
            max_offset_bps,
            tick_size,
            active_position: None,
            trailing_stop_ticks: 4, // Откат на 4 тика от пика для срабатывания стопа
            target_profit_ticks: 8, // Цель по прибыли в 8 тиков
            csv_filename: filename,
        }
    }

    /// Расчет микроцены по динамическому ценовому диапазону (фильтрация стаканного шума)
    fn calculate_window_weighted_micro_price(&self, book: &OrderBook) -> Option<(u64, u64, u64)> {
        let (best_bid, _) = book.best_bid()?;
        let (best_ask, _) = book.best_ask()?;
        let mid_price = best_bid + (best_ask - best_bid) / 2;
        if mid_price == 0 {
            return None;
        }

        // Получаем глубокий срез стакана
        let mut bids_buf = vec![(0, 0); 50];
        let mut asks_buf = vec![(0, 0); 50];
        let (b_len, a_len) = book.copy_top_levels(&mut bids_buf, &mut asks_buf);

        let mut total_bid_vol: u64 = 0;
        let mut total_ask_vol: u64 = 0;

        // Суммируем объемы бидов внутри заданного ценового окна (в базисных пунктах)
        for &(p, v) in &bids_buf[..b_len] {
            let offset_bps = ((mid_price.saturating_sub(p)) * 10_000) / mid_price;
            if offset_bps <= self.max_offset_bps {
                let weight = self.max_offset_bps.saturating_sub(offset_bps) + 1;
                total_bid_vol += v * weight;
            }
        }

        // Суммируем объемы асков внутри ценового окна
        for &(p, v) in &asks_buf[..a_len] {
            let offset_bps = ((p.saturating_sub(mid_price)) * 10_000) / mid_price;
            if offset_bps <= self.max_offset_bps {
                let weight = self.max_offset_bps.saturating_sub(offset_bps) + 1;
                total_ask_vol += v * weight;
            }
        }

        let total_vol = total_bid_vol + total_ask_vol;
        if total_vol == 0 {
            return Some((best_bid, best_ask, mid_price));
        }

        let spread = best_ask - best_bid;
        let weighted_offset = (spread * total_ask_vol) / total_vol;
        let micro_price = best_bid + weighted_offset;

        Some((best_bid, best_ask, micro_price))
    }

    /// Запись результатов закрытой сделки в CSV
    fn log_trade(&self, timestamp_ns: u64, side: Side, entry: u64, exit: u64, qty: u64, pnl: i64) {
        if let Ok(mut file) = OpenOptions::new().append(true).open(&self.csv_filename) {
            let side_str = match side { Side::Bid => "BUY", Side::Ask => "SELL" };
            let _ = writeln!(file, "{},{},{},{},{},{}", timestamp_ns, side_str, entry, exit, qty, pnl);
        }
    }
}

impl Strategy for AdvancedScalper {
    fn name(&self) -> &'static str {
        "AdvancedWindowScalper"
    }

    fn evaluate(&mut self, book: &OrderBook, risk: &RiskEngine, now_ns: u64) -> Option<Signal> {
        let (best_bid, best_ask, micro_price) = self.calculate_window_weighted_micro_price(book)?;
        let mid_price = best_bid + (best_ask - best_bid) / 2;
        let current_position = risk.position();

        // 1. Управление активной позицией (Trailing Stop & Take Profit)
        if let Some((side, entry_price, ref mut best_price)) = self.active_position {
            match side {
                Side::Bid => {
                    if best_bid > *best_price {
                        *best_price = best_bid; // Обновляем пик цены вверх
                    }
                    let stop_price = best_price.saturating_sub(self.trailing_stop_ticks * self.tick_size);
                    let target_price = entry_price + self.target_profit_ticks * self.tick_size;

                    if best_bid <= stop_price || best_bid >= target_price {
                        let exit_price = best_bid;
                        let pnl = ((exit_price as i64) - (entry_price as i64)) * (self.order_size as i64);
                        self.log_trade(now_ns, Side::Ask, entry_price, exit_price, self.order_size, pnl);
                        risk.record_pnl(pnl);
                        risk.record_fill(Side::Ask, self.order_size);
                        self.active_position = None;
                    }
                }
                Side::Ask => {
                    if best_ask < *best_price {
                        *best_price = best_ask; // Обновляем пик цены вниз
                    }
                    let stop_price = *best_price + self.trailing_stop_ticks * self.tick_size;
                    let target_price = entry_price.saturating_sub(self.target_profit_ticks * self.tick_size);

                    if best_ask >= stop_price || best_ask <= target_price {
                        let exit_price = best_ask;
                        let pnl = ((entry_price as i64) - (exit_price as i64)) * (self.order_size as i64);
                        self.log_trade(now_ns, Side::Bid, entry_price, exit_price, self.order_size, pnl);
                        risk.record_pnl(pnl);
                        risk.record_fill(Side::Bid, self.order_size);
                        self.active_position = None;
                    }
                }
            }
            return None;
        }

        // 2. Поиск точки входа
        let can_buy = current_position < self.max_inventory;
        let can_sell = current_position > -self.max_inventory;

        if micro_price > mid_price && can_buy {
            if risk.check_order(Side::Bid, self.order_size, now_ns).is_ok() {
                self.active_position = Some((Side::Bid, best_ask, best_ask));
                risk.record_fill(Side::Bid, self.order_size);
                return Some(Signal {
                    side: Side::Bid,
                    price: best_ask,
                    quantity: self.order_size,
                });
            }
        } else if micro_price < mid_price && can_sell {
            if risk.check_order(Side::Ask, self.order_size, now_ns).is_ok() {
                self.active_position = Some((Side::Ask, best_bid, best_bid));
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
        let mut scalper = AdvancedScalper::new(2, 10, 10, 1); // 10 bps окно, tick_size = 1

        // Наполняем стакан дисбалансом
        book.update_level(Side::Bid, 10000, 500);
        book.update_level(Side::Ask, 10002, 50);

        let signal = scalper.evaluate(&book, &risk, 1000);
        assert!(signal.is_some());
        let sig = signal.unwrap();
        assert_eq!(sig.side, Side::Bid);
    }
}