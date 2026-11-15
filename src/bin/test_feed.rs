use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use std::time::Instant;

// Минимальные структуры только для парсинга стакана Bybit V5
#[derive(Debug, Deserialize)]
pub struct BybitOrderBookMsg {
    pub topic: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: OrderBookData,
}

#[derive(Debug, Deserialize)]
pub struct OrderBookData {
    pub s: String,              // Символ (например, BTCUSDT)
    pub b: Vec<(String, String)>, // Bids: [[price, size], ...]
    pub a: Vec<(String, String)>, // Asks: [[price, size], ...]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let symbol = "BTCUSDT";
    let url = "wss://stream.bybit.com/v5/public/linear";

    println!("🔌 Подключаемся к Bybit WebSocket для теста потока ({}) ...", symbol);
    let (mut ws_stream, _) = connect_async(url).await?;
    println!("✅ Успешно подключено!");

    // Отправляем подписку на стакан (топ-50 уровней)
    let subscribe_msg = serde_json::json!({
        "op": "subscribe",
        "args": [format!("orderbook.50.{}", symbol)]
    });

    ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
    println!("📡 Запрос подписки отправлен. Ожидаем данные стакана (нажмите Ctrl+C для выхода)...\n");

    let mut message_count = 0;
    let mut last_print = Instant::now();

    // Читаем поток сообщений
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // Пытаемся распарсить сообщение стакана
                if let Ok(ob_msg) = serde_json::from_str::<BybitOrderBookMsg>(&text) {
                    if ob_msg.topic.starts_with("orderbook") {
                        message_count += 1;

                        // Выводим в консоль раз в секунду, чтобы не заспамить экран
                        if last_print.elapsed().as_secs() >= 1 {
                            print!("\x1B[2J\x1B[1H"); // Очистка экрана терминала
                            println!("==================================================");
                            println!("📊 ТЕСТОВЫЙ ФИД BYBIT V5 [{}] — Получено сообщений: {}", symbol, message_count);
                            println!("Тип сообщения: {} | Элементов в Бидах: {} | В Асках: {}", 
                                ob_msg.msg_type, ob_msg.data.b.len(), ob_msg.data.a.len());
                            println!("==================================================");

                            // Показываем топ-3 Бида (покупатели)
                            println!("--- TOP 3 BIDS (Покупка) ---");
                            for (price, size) in ob_msg.data.b.iter().take(3) {
                                println!("  Цена: {:<10} | Объём: {}", price, size);
                            }

                            // Показываем топ-3 Аска (продавцы)
                            println!("--- TOP 3 ASKS (Продажа) ---");
                            for (price, size) in ob_msg.data.a.iter().take(3) {
                                println!("  Цена: {:<10} | Объём: {}", price, size);
                            }
                            println!("==================================================");

                            last_print = Instant::now();
                        }
                    }
                }
            }
            Ok(Message::Ping(ping)) => {
                // Автоматически отвечаем на пинг биржи, чтобы соединение не рвалось
                let _ = ws_stream.send(Message::Pong(ping)).await;
            }
            Err(e) => {
                eprintln!("❌ Ошибка потока WebSocket: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}