mod ring_buffer;
mod order_book;
mod strategy;
mod risk;
mod ws_client;

use order_book::OrderBook;
use risk::{RiskEngine, RiskConfig};
use strategy::AdvancedScalper;
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();
    println!("=== HFT Platform: Live Data Collection Mode (Bybit WebSocket) ===");

    // 1. Инициализация компонентов ядра
    let book = OrderBook::new();
    let config = RiskConfig::new(10, 100, 100000, 50, Duration::from_secs(1)).unwrap();
    let risk = RiskEngine::new(config);
    
    // Стратегия: размер ордера 2, макс позиция 10, окно 10 bps, размер тика 1
    let strategy = AdvancedScalper::new(2, 10, 10, 1);

    let symbol = "BTCUSDT".to_string();

    // 2. Запуск WebSocket потока сбора данных и анализа
    ws_client::run_bybit_feed(symbol, book, strategy, risk).await;
}