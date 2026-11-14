use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    // Bids (покупки): сортируются по возрастанию ключа в BTreeMap. 
    // Лучший бид (Best Bid) находится в самом конце (.iter().next_back()).
    bids: BTreeMap<u64, u64>,

    // Asks (продажи): сортируются по возрастанию ключа в BTreeMap.
    // Лучший аск (Best Ask) находится в самом начале (.iter().next()).
    asks: BTreeMap<u64, u64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Очистка стакана (например, при реконекте или snapshot)
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Обновление ценового уровня. 
    /// Если volume == 0, уровень удаляется из книги.
    #[inline]
    pub fn update_level(&mut self, side: Side, price: u64, volume: u64) {
        let book = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        if volume == 0 {
            book.remove(&price);
        } else {
            book.insert(price, volume);
        }
    }

    /// Возвращает лучший Бид (Цена, Объем) — максимальная цена покупки
    #[inline]
    pub fn best_bid(&self) -> Option<(u64, u64)> {
        self.bids.iter().next_back().map(|(&p, &v)| (p, v))
    }

    /// Возвращает лучший Аск (Цена, Объем) — минимальная цена продажи
    #[inline]
    pub fn best_ask(&self) -> Option<(u64, u64)> {
        self.asks.iter().next().map(|(&p, &v)| (p, v))
    }

    /// Расчет текущего спреда (Best Ask - Best Bid).
    /// Поддерживает Locked Market (когда ask == bid, спред равен 0).
    #[inline]
    pub fn spread(&self) -> Option<u64> {
        let (best_bid, _) = self.best_bid()?;
        let (best_ask, _) = self.best_ask()?;
        
        if best_ask >= best_bid {
            Some(best_ask - best_bid)
        } else {
            // Crossed market (аск ниже бида) — аномалия стакана
            None
        }
    }

    /// Получение топ-N уровней стакана БЕЗ динамических аллокаций памяти (Zero-Allocation).
    /// Заполняет переданные извне срезы (bids и asks) и возвращает количество записанных элементов.
    pub fn copy_top_levels(&self, bids: &mut [(u64, u64)], asks: &mut [(u64, u64)]) -> (usize, usize) {
        let mut bids_len = 0;
        for ((&p, &v), slot) in self.bids.iter().rev().zip(bids.iter_mut()) {
            *slot = (p, v);
            bids_len += 1;
        }

        let mut asks_len = 0;
        for ((&p, &v), slot) in self.asks.iter().zip(asks.iter_mut()) {
            *slot = (p, v);
            asks_len += 1;
        }

        (bids_len, asks_len)
    }

    /// Устаревший метод с аллокациями. Оставлен для обратной совместимости в тестах.
    pub fn get_top_levels(&self, depth: usize) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let mut bids_buf = vec![(0, 0); depth];
        let mut asks_buf = vec![(0, 0); depth];
        let (b_len, a_len) = self.copy_top_levels(&mut bids_buf, &mut asks_buf);
        bids_buf.truncate(b_len);
        asks_buf.truncate(a_len);
        (bids_buf, asks_buf)
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_book_basic_operations() {
        let mut book = OrderBook::new();

        book.update_level(Side::Bid, 100050, 10);
        book.update_level(Side::Bid, 100060, 5); // Лучший бид

        book.update_level(Side::Ask, 100070, 8); // Лучший аск
        book.update_level(Side::Ask, 100080, 20);

        assert_eq!(book.best_bid(), Some((100060, 5)));
        assert_eq!(book.best_ask(), Some((100070, 8)));
        assert_eq!(book.spread(), Some(10)); // 100070 - 100060

        // Проверка Locked Market (спред = 0)
        book.update_level(Side::Ask, 100060, 2);
        assert_eq!(book.spread(), Some(0));

        // Удаление уровня (volume = 0)
        book.update_level(Side::Ask, 100060, 0);
        assert_eq!(book.best_ask(), Some((100070, 8)));
    }

    #[test]
    fn test_copy_top_levels_no_alloc() {
        let mut book = OrderBook::new();
        
        book.update_level(Side::Ask, 101, 1);
        book.update_level(Side::Ask, 102, 2);
        book.update_level(Side::Bid, 100, 5);

        let mut bids = [(0, 0); 5];
        let mut asks = [(0, 0); 5];

        let (b_len, a_len) = book.copy_top_levels(&mut bids, &mut asks);

        assert_eq!(b_len, 1);
        assert_eq!(a_len, 2);
        assert_eq!(bids[0], (100, 5));
        assert_eq!(asks[0], (101, 1));
        assert_eq!(asks[1], (102, 2));
    }
}