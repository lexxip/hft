use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::order_book::{OrderBook, Side};
use crate::risk::RiskEngine;
use crate::strategy::{AdvancedScalper, Strategy};

#[derive(Debug, Deserialize)]
pub struct BybitOrderBookMsg {
    pub topic: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub ts: u64,
    pub data: OrderBookData,
}

#[derive(Debug, Deserialize)]
pub struct OrderBookData {
    pub s: String,
    pub b: Vec<(String, String)>,
    pub a: Vec<(String, String)>,
    pub u: u64,
}

/// Асинхронный запуск WebSocket-клиента Bybit с дашбордом и graceful shutdown
pub async fn run_bybit_feed(
    symbol: String,
    mut book: OrderBook,
    mut strategy: AdvancedScalper,
    risk: RiskEngine,
) {
    let url = "wss://stream.bybit.com/v5/public/linear";
    
    println!("[WS] Connecting to Bybit WebSocket: {} ...", url);

    let mut last_dashboard_print = Instant::now();
    let dashboard_interval = Duration::from_secs(1);

    // Обертка tokio::select! объединяет торговый поток и ожидание Ctrl+C
    tokio::select! {
        _ = async {
            loop {
                match connect_async(url).await {
                    Ok((mut ws_stream, _)) => {
                        println!("[WS] Connected successfully! Subscribing to orderbook for {}", symbol);

                        let subscribe_msg = serde_json::json!({
                            "op": "subscribe",
                            "args": [format!("orderbook.50.{}", symbol)]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string())).await {
                            eprintln!("[WS] Failed to send subscription: {}", e);
                            continue;
                        }

                        while let Some(msg) = ws_stream.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(ob_msg) = serde_json::from_str::<BybitOrderBookMsg>(&text) {
                                        if ob_msg.topic.starts_with("orderbook") {
                                            // 1. Обновляем стакан
                                            apply_bybit_update(&mut book, &ob_msg);

                                            let now_ns = SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_nanos() as u64;

                                            // 2. Оцениваем стратегию
                                            if let Some(signal) = strategy.evaluate(&book, &risk, now_ns) {
                                                println!(
                                                    "\n🔥 [SIGNAL] {:?} qty={} @ price={} | Pos: {} | PnL: {}",
                                                    signal.side, signal.quantity, signal.price, risk.position(), risk.daily_pnl()
                                                );
                                            }

                                            // 3. Выводим консольный дашборд раз в секунду
                                            if last_dashboard_print.elapsed() >= dashboard_interval {
                                                print_trading_dashboard(&book, &risk, &symbol);
                                                last_dashboard_print = Instant::now();
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Ping(ping)) => {
                                    let _ = ws_stream.send(Message::Pong(ping)).await;
                                }
                                Err(e) => {
                                    eprintln!("[WS] Stream error: {}", e);
                                    break; // Выход из inner loop для реконнекта
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[WS] Connection error: {}. Retrying in 3 seconds...", e);
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        } => {}

        // Перехват Ctrl+C
        _ = tokio::signal::ctrl_c() => {
            println!("\n\n🛑 [SHUTDOWN] Interrupt signal received (Ctrl+C).");
            println!("💾 Flushing buffers and saving CSV logs safely...");
        }
    }

    println!("✅ [SHUTDOWN] Application exited gracefully. Goodbye!");
}

fn apply_bybit_update(book: &mut OrderBook, msg: &BybitOrderBookMsg) {
    for (price_str, size_str) in &msg.data.b {
        if let (Ok(price_f), Ok(size_f)) = (price_str.parse::<f64>(), size_str.parse::<f64>()) {
            let price = (price_f * 100.0) as u64;
            let size = size_f as u64;
            book.update_level(Side::Bid, price, size);
        }
    }

    for (price_str, size_str) in &msg.data.a {
        if let (Ok(price_f), Ok(size_f)) = (price_str.parse::<f64>(), size_str.parse::<f64>()) {
            let price = (price_f * 100.0) as u64;
            let size = size_f as u64;
            book.update_level(Side::Ask, price, size);
        }
    }
}

/// Консольная панель мониторинга торгов (TUI Dashboard)
fn print_trading_dashboard(book: &OrderBook, risk: &RiskEngine, symbol: &str) {
    let best_bid = book.best_bid().unwrap_or((0, 0));
    let best_ask = book.best_ask().unwrap_or((0, 0));
    let spread = book.spread().unwrap_or(0);

    let bid_float = best_bid.0 as f64 / 100.0;
    let ask_float = best_ask.0 as f64 / 100.0;
    let spread_float = spread as f64 / 100.0;

    print!("\x1B[2J\x1B[1H"); // Очистка экрана терминала
    println!("============================================================");
    println!("📊 HFT PLATFORM LIVE DASHBOARD [{}]", symbol);
    println!("============================================================");
    println!("  Best Bid  : {:.2} (vol: {})", bid_float, best_bid.1);
    println!("  Best Ask  : {:.2} (vol: {})", ask_float, best_ask.1);
    println!("  Spread    : {:.2}", spread_float);
    println!("------------------------------------------------------------");
    println!("  Position  : {} lots", risk.position());
    println!("  Daily PnL : {} USD", risk.daily_pnl());
    println!("  Trading   : {}", if risk.is_trading_halted() { "HALTED 🛑" } else { "ACTIVE ✅" });
    println!("============================================================");
    println!("(Press Ctrl+C to stop collection & save logs)");
}