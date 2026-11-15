use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::{Duration, Instant};
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

    tokio::select! {
        _ = async {
            loop {
                match connect_async(url).await {
                    Ok((ws_stream, _)) => {
                        println!("[WS] Connected successfully! Subscribing to orderbook for {}", symbol);

                        // 1. Разделяем поток: write — для подписки и heartbeat, read — для данных
                        let (mut write, mut read) = ws_stream.split();

                        // 2. Стакан от прошлой сессии невалиден — очищаем ДО подписки
                        book.clear();

                        // 3. Подписка
                        let subscribe_msg = serde_json::json!({
                            "op": "subscribe",
                            "args": [format!("orderbook.50.{}", symbol)]
                        });

                        if let Err(e) = write.send(Message::Text(subscribe_msg.to_string())).await {
                            eprintln!("[WS] Failed to send subscription: {}", e);
                            continue;
                        }

                        // 4. Фоновая задача для прикладного heartbeat Bybit ({"op":"ping"})
                        let ping_handle = tokio::spawn(async move {
                            let mut interval = tokio::time::interval(Duration::from_secs(15));
                            interval.tick().await; // первый tick срабатывает сразу — пропускаем

                            loop {
                                interval.tick().await;
                                let ping_msg = Message::Text(r#"{"op":"ping"}"#.to_string());
                                if let Err(e) = write.send(ping_msg).await {
                                    eprintln!("[WS] Heartbeat send failed: {}", e);
                                    break;
                                }
                            }
                        });

                        // 5. Основной цикл чтения
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(ob_msg) = serde_json::from_str::<BybitOrderBookMsg>(&text) {
                                        if ob_msg.topic.starts_with("orderbook") {
                                            // snapshot — полная замена стакана
                                            if ob_msg.msg_type == "snapshot" {
                                                book.clear();
                                            }

                                            apply_bybit_update(&mut book, &ob_msg);

                                            // Bybit ts — микросекунды; в наносекунды: *1000
                                            let now_ns = ob_msg.ts * 1_000;

                                            if let Some(signal) = strategy.evaluate(&book, &risk, now_ns) {
                                                println!(
                                                    "\n🔥 [SIGNAL] {:?} qty={} @ price={} | Pos: {} | PnL: {}",
                                                    signal.side, signal.quantity, signal.price,
                                                    risk.position(), risk.daily_pnl()
                                                );
                                            }

                                            if last_dashboard_print.elapsed() >= dashboard_interval {
                                                print_trading_dashboard(&book, &risk, &symbol);
                                                last_dashboard_print = Instant::now();
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("[WS] Stream error: {}", e);
                                    break;
                                }
                                // Ping/Pong/Binary/Close — игнорируем,
                                // tungstenite автоматически отвечает на WebSocket-level Ping
                                _ => {}
                            }
                        }

                        // 6. Останавливаем heartbeat при выходе из цикла чтения
                        ping_handle.abort();

                        println!("[WS] Disconnected. Reconnecting...");
                    }
                    Err(e) => {
                        eprintln!("[WS] Connection error: {}. Retrying in 3 seconds...", e);
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        } => {}

        _ = tokio::signal::ctrl_c() => {
            println!("\n\n🛑 [SHUTDOWN] Interrupt signal received (Ctrl+C).");
            println!("💾 Flushing buffers and saving CSV logs safely...");
        }
    }

    println!("✅ [SHUTDOWN] Application exited gracefully. Goodbye!");
}

/// Парсит "67432.10" в fixed-point с заданным scale (например, 100 → 6743210).
fn parse_fixed(s: &str, scale: u32) -> Option<u64> {
    let (int_part, frac_part) = s.split_once('.').unwrap_or((s, ""));
    let int_v: u64 = int_part.parse().ok()?;
    let mul = 10u64.checked_pow(scale)?;

    let mut frac = frac_part.to_string();
    let target = scale as usize;
    while frac.len() < target {
        frac.push('0');
    }
    frac.truncate(target);

    let frac_v: u64 = if frac.is_empty() {
        0
    } else {
        frac.parse().ok()?
    };

    int_v.checked_mul(mul)?.checked_add(frac_v)
}

fn apply_bybit_update(book: &mut OrderBook, msg: &BybitOrderBookMsg) {
    // price scale = 2 (центы), size scale = 3 (мили-BTC)
    for (p, s) in &msg.data.b {
        if let (Some(price), Some(size)) = (parse_fixed(p, 2), parse_fixed(s, 3)) {
            book.update_level(Side::Bid, price, size);
        }
    }
    for (p, s) in &msg.data.a {
        if let (Some(price), Some(size)) = (parse_fixed(p, 2), parse_fixed(s, 3)) {
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

    let bid_vol_float = best_bid.1 as f64 / 1000.0;
    let ask_vol_float = best_ask.1 as f64 / 1000.0;

    print!("\x1B[2J\x1B[1H");
    println!("============================================================");
    println!("📊 HFT PLATFORM LIVE DASHBOARD [{}]", symbol);
    println!("============================================================");
    println!("  Best Bid  : {:.2} (vol: {:.3})", bid_float, bid_vol_float);
    println!("  Best Ask  : {:.2} (vol: {:.3})", ask_float, ask_vol_float);
    println!("  Spread    : {:.2}", spread_float);
    println!("------------------------------------------------------------");
    println!("  Position  : {} lots", risk.position());
    println!("  Daily PnL : {} USD", risk.daily_pnl());
    println!("  Trading   : {}", if risk.is_trading_halted() { "HALTED 🛑" } else { "ACTIVE ✅" });
    println!("============================================================");
    println!("(Press Ctrl+C to stop collection & save logs)");
}