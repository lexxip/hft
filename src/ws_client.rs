use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::order_book::{OrderBook, Side};
use crate::risk::RiskEngine;
use crate::strategy::{AdvancedScalper, Strategy};

// Структуры для десериализации сообщений Bybit V5 L2 Orderbook
#[derive(Debug, Deserialize)]
pub struct BybitOrderBookMsg {
    pub topic: String,
    #[serde(rename = "type")]
    pub msg_type: String, // "snapshot" или "delta"
    pub ts: u64,
    pub data: OrderBookData,
}

#[derive(Debug, Deserialize)]
pub struct OrderBookData {
    pub s: String,              // Символ, например "BTCUSDT"
    pub b: Vec<(String, String)>, // Bids: [[price, size], ...]
    pub a: Vec<(String, String)>, // Asks: [[price, size], ...]
    pub u: u64,                 // Update ID
}

/// Функция запуска WebSocket-клиента в отдельном Async-потоке
pub async fn run_bybit_feed(
    symbol: String,
    mut book: OrderBook,
    mut strategy: AdvancedScalper,
    risk: RiskEngine,
) {
    // Публичный эндпоинт Bybit V5 для деривативов/спота (Linear & Spot)
    let url = "wss://stream.bybit.com/v5/public/linear";
    
    println!("[WS] Connecting to Bybit WebSocket: {} ...", url);

    loop {
        match connect_async(url).await {
            Ok((mut ws_stream, _)) => {
                println!("[WS] Connected successfully! Subscribing to orderbook for {}", symbol);

                // Формируем запрос на подписку (топ-50 уровней стакана)
                let subscribe_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": [format!("orderbook.50.{}", symbol)]
                });

                if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string())).await {
                    eprintln!("[WS] Failed to send subscription: {}", e);
                    continue;
                }

                // Читаем поток сообщений в бесконечном цикле
                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(ob_msg) = serde_json::from_str::<BybitOrderBookMsg>(&text) {
                                if ob_msg.topic.starts_with("orderbook") {
                                    // 1. Обновляем локальный стакан
                                    apply_bybit_update(&mut book, &ob_msg);

                                    // 2. Получаем текущее время в наносекундах
                                    let now_ns = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_nanos() as u64;

                                    // 3. Передаем стакан и риски в стратегию
                                    if let Some(signal) = strategy.evaluate(&book, &risk, now_ns) {
                                        println!(
                                            "🔥 [SIGNAL] Generated: {:?} qty={} @ price={} | Position: {}",
                                            signal.side, signal.quantity, signal.price, risk.position()
                                        );
                                        // В режиме Paper Trading ордер не уходит на биржу через REST,
                                        // а фиксируется риск-движком и логгируется в CSV.
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(ping)) => {
                            let _ = ws_stream.send(Message::Pong(ping)).await;
                        }
                        Err(e) => {
                            eprintln!("[WS] Stream error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("[WS] Connection error: {}. Retrying in 3 seconds...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
        }
    }
}

/// Функция применения снапшота или дельты от Bybit к нашему OrderBook
fn apply_bybit_update(book: &mut OrderBook, msg: &BybitOrderBookMsg) {
    if msg.msg_type == "snapshot" {
        // При снапшоте можно очистить стакан, если это требуется логикой реконнекта
        // book.clear(); 
    }

    // Обработка Бидов (Покупки)
    for (price_str, size_str) in &msg.data.b {
        if let (Ok(price_f), Ok(size_f)) = (price_str.parse::<f64>(), size_str.parse::<f64>()) {
            // Переводим цену в фиксированную точку (умножаем на 100 или 10000 в зависимости от точности инструмента)
            // Для BTCUSDT с шагом 0.01 умножаем на 100 для получения u64
            let price = (price_f * 100.0) as u64;
            let size = size_f as u64;
            book.update_level(Side::Bid, price, size);
        }
    }

    // Обработка Асков (Продажи)
    for (price_str, size_str) in &msg.data.a {
        if let (Ok(price_f), Ok(size_f)) = (price_str.parse::<f64>(), size_str.parse::<f64>()) {
            let price = (price_f * 100.0) as u64;
            let size = size_f as u64;
            book.update_level(Side::Ask, price, size);
        }
    }
}