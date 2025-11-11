/// Two-Trader Competition: Momentum vs Mean Reversion
///
/// This example demonstrates a realistic trading competition between two sophisticated
/// traders with different strategies competing in the same market.
///
/// TRADER 1: "Momentum Hunter" - Directional trader
/// - Identifies trends and momentum
/// - Aggressive position sizing on conviction
/// - Uses recent price action and volume
/// - Quotes with directional bias
///
/// TRADER 2: "Market Maven" - Mean reversion market maker
/// - Provides liquidity and captures spread
/// - Fades extremes and reverts to mean
/// - Manages inventory risk carefully
/// - Tighter quotes, more frequent updates
///
/// Starting capital: $1,000,000 each
/// Stop condition: First trader to lose 20% ($800,000 equity)
///
/// Market dynamics:
/// - Realistic bid-ask spreads
/// - Order book depth
/// - Price discovery through competition
/// - Transaction costs
/// - Slippage simulation

use dolda::mailbox::MailboxSystem;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use tokio::time::{sleep, Duration, Instant};
use tracing::info;

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MessageType {
    Quote(Quote),
    Order(Order),
    Trade(Trade),
    MarketUpdate(MarketUpdate),
    PositionReport(PositionReport),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Quote {
    trader_id: String,
    symbol: String,
    bid_price: f64,
    bid_size: i64,
    ask_price: f64,
    ask_size: i64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    trader_id: String,
    symbol: String,
    side: Side,
    order_type: OrderType,
    price: f64,
    quantity: i64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum OrderType {
    Limit,
    Market,
    IOC,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Trade {
    trade_id: String,
    symbol: String,
    buyer_id: String,
    seller_id: String,
    price: f64,
    quantity: i64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketUpdate {
    symbol: String,
    last_price: f64,
    bid_price: f64,
    ask_price: f64,
    volume: i64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PositionReport {
    trader_id: String,
    cash: f64,
    positions: HashMap<String, i64>,
    realized_pnl: f64,
    unrealized_pnl: f64,
    total_equity: f64,
    timestamp: u64,
}

// ============================================================================
// Order Book - Central matching engine
// ============================================================================

struct OrderBook {
    symbol: String,
    bids: VecDeque<(f64, i64, String)>, // (price, quantity, trader_id)
    asks: VecDeque<(f64, i64, String)>,
    last_trade_price: f64,
    trades: Vec<Trade>,
    trade_counter: u64,
}

impl OrderBook {
    fn new(symbol: String, initial_price: f64) -> Self {
        Self {
            symbol,
            bids: VecDeque::new(),
            asks: VecDeque::new(),
            last_trade_price: initial_price,
            trades: Vec::new(),
            trade_counter: 0,
        }
    }

    fn update_quote(&mut self, quote: Quote) {
        // Remove old quotes from this trader
        self.bids.retain(|(_, _, id)| id != &quote.trader_id);
        self.asks.retain(|(_, _, id)| id != &quote.trader_id);

        // Add new quotes
        if quote.bid_size > 0 {
            self.bids.push_back((quote.bid_price, quote.bid_size, quote.trader_id.clone()));
            self.bids.make_contiguous().sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        }
        if quote.ask_size > 0 {
            self.asks.push_back((quote.ask_price, quote.ask_size, quote.trader_id.clone()));
            self.asks.make_contiguous().sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        }
    }

    fn match_order(&mut self, order: Order) -> Vec<Trade> {
        let mut trades = Vec::new();
        let mut remaining_qty = order.quantity;

        match order.side {
            Side::Buy => {
                while remaining_qty > 0 && !self.asks.is_empty() {
                    let (ask_price, ask_qty, seller_id) = self.asks[0].clone();
                    
                    // Check if prices cross
                    if order.order_type == OrderType::Market || order.price >= ask_price {
                        let trade_qty = remaining_qty.min(ask_qty);
                        
                        self.trade_counter += 1;
                        let trade = Trade {
                            trade_id: format!("T{}", self.trade_counter),
                            symbol: order.symbol.clone(),
                            buyer_id: order.trader_id.clone(),
                            seller_id: seller_id.clone(),
                            price: ask_price,
                            quantity: trade_qty,
                            timestamp: order.timestamp,
                        };
                        
                        trades.push(trade.clone());
                        self.trades.push(trade);
                        self.last_trade_price = ask_price;
                        
                        remaining_qty -= trade_qty;
                        
                        if trade_qty >= ask_qty {
                            self.asks.pop_front();
                        } else {
                            self.asks[0].1 -= trade_qty;
                        }
                    } else {
                        break;
                    }
                }
            }
            Side::Sell => {
                while remaining_qty > 0 && !self.bids.is_empty() {
                    let (bid_price, bid_qty, buyer_id) = self.bids[0].clone();
                    
                    if order.order_type == OrderType::Market || order.price <= bid_price {
                        let trade_qty = remaining_qty.min(bid_qty);
                        
                        self.trade_counter += 1;
                        let trade = Trade {
                            trade_id: format!("T{}", self.trade_counter),
                            symbol: order.symbol.clone(),
                            buyer_id: buyer_id.clone(),
                            seller_id: order.trader_id.clone(),
                            price: bid_price,
                            quantity: trade_qty,
                            timestamp: order.timestamp,
                        };
                        
                        trades.push(trade.clone());
                        self.trades.push(trade);
                        self.last_trade_price = bid_price;
                        
                        remaining_qty -= trade_qty;
                        
                        if trade_qty >= bid_qty {
                            self.bids.pop_front();
                        } else {
                            self.bids[0].1 -= trade_qty;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        // Add remaining quantity to book if it's a limit order
        if remaining_qty > 0 && order.order_type == OrderType::Limit {
            match order.side {
                Side::Buy => {
                    self.bids.push_back((order.price, remaining_qty, order.trader_id));
                    self.bids.make_contiguous().sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                }
                Side::Sell => {
                    self.asks.push_back((order.price, remaining_qty, order.trader_id));
                    self.asks.make_contiguous().sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                }
            }
        }

        trades
    }

    fn get_market_update(&self, timestamp: u64) -> MarketUpdate {
        let bid_price = self.bids.front().map(|(p, _, _)| *p).unwrap_or(self.last_trade_price - 0.10);
        let ask_price = self.asks.front().map(|(p, _, _)| *p).unwrap_or(self.last_trade_price + 0.10);
        
        MarketUpdate {
            symbol: self.symbol.clone(),
            last_price: self.last_trade_price,
            bid_price,
            ask_price,
            volume: self.trades.iter().map(|t| t.quantity).sum(),
            timestamp,
        }
    }

    fn get_spread(&self) -> f64 {
        if let (Some((bid, _, _)), Some((ask, _, _))) = (self.bids.front(), self.asks.front()) {
            ask - bid
        } else {
            0.10 // Default spread
        }
    }
}

// ============================================================================
// Trader 1: Momentum Hunter
// ============================================================================

struct MomentumTrader {
    trader_id: String,
    cash: f64,
    positions: HashMap<String, i64>,
    realized_pnl: f64,
    price_history: VecDeque<(u64, f64)>, // (timestamp, price)
    trade_history: VecDeque<Trade>,
    order_counter: u64,
}

impl MomentumTrader {
    fn new(trader_id: String, initial_cash: f64) -> Self {
        Self {
            trader_id,
            cash: initial_cash,
            positions: HashMap::new(),
            realized_pnl: 0.0,
            price_history: VecDeque::new(),
            trade_history: VecDeque::new(),
            order_counter: 0,
        }
    }

    fn update_market(&mut self, update: &MarketUpdate) {
        self.price_history.push_back((update.timestamp, update.last_price));
        if self.price_history.len() > 100 {
            self.price_history.pop_front();
        }
    }

    fn calculate_momentum(&self) -> f64 {
        if self.price_history.len() < 20 {
            return 0.0;
        }

        let recent: Vec<f64> = self.price_history.iter().rev().take(20).map(|(_, p)| *p).collect();
        let older: Vec<f64> = self.price_history.iter().rev().skip(20).take(20).map(|(_, p)| *p).collect();

        if older.is_empty() {
            return 0.0;
        }

        let recent_avg = recent.iter().sum::<f64>() / recent.len() as f64;
        let older_avg = older.iter().sum::<f64>() / older.len() as f64;

        (recent_avg - older_avg) / older_avg
    }

    fn generate_quote(&mut self, market: &MarketUpdate, timestamp: u64) -> Quote {
        let momentum = self.calculate_momentum();
        let position = self.positions.get(&market.symbol).copied().unwrap_or(0);
        
        // Base spread
        let spread = 0.05;
        
        // Adjust for momentum
        let bias = momentum * 0.50; // Bias quotes in momentum direction
        
        // Adjust for position (reduce risk if large position)
        let position_adjustment = (position as f64 / 1000.0) * 0.02;
        
        let mid = market.last_price;
        let bid_price = mid - spread / 2.0 + bias - position_adjustment;
        let ask_price = mid + spread / 2.0 + bias - position_adjustment;
        
        // Size based on conviction
        let conviction = momentum.abs();
        let base_size = 100;
        let size = (base_size as f64 * (1.0 + conviction * 2.0)) as i64;
        
        Quote {
            trader_id: self.trader_id.clone(),
            symbol: market.symbol.clone(),
            bid_price: (bid_price * 100.0).round() / 100.0,
            bid_size: size,
            ask_price: (ask_price * 100.0).round() / 100.0,
            ask_size: size,
            timestamp,
        }
    }

    fn generate_order(&mut self, market: &MarketUpdate, timestamp: u64) -> Option<Order> {
        let momentum = self.calculate_momentum();
        let position = self.positions.get(&market.symbol).copied().unwrap_or(0);
        
        // Strong momentum signal and not overleveraged (lowered threshold for more action)
        if momentum.abs() > 0.008 && position.abs() < 2000 {
            let conviction_size = (momentum.abs() * 5000.0) as i64;
            let size = conviction_size.min(500).max(100);
            
            // Risk check
            let required_cash = size as f64 * market.last_price;
            if self.cash < required_cash * 0.5 {
                return None;
            }
            
            self.order_counter += 1;
            
            if momentum > 0.0 && position < 2000 {
                // Buy on upward momentum
                Some(Order {
                    order_id: format!("{}-O{}", self.trader_id, self.order_counter),
                    trader_id: self.trader_id.clone(),
                    symbol: market.symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::IOC,
                    price: market.ask_price + 0.02, // Aggressive
                    quantity: size,
                    timestamp,
                })
            } else if momentum < 0.0 && position > -2000 {
                // Sell on downward momentum
                Some(Order {
                    order_id: format!("{}-O{}", self.trader_id, self.order_counter),
                    trader_id: self.trader_id.clone(),
                    symbol: market.symbol.clone(),
                    side: Side::Sell,
                    order_type: OrderType::IOC,
                    price: market.bid_price - 0.02, // Aggressive
                    quantity: size,
                    timestamp,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn process_trade(&mut self, trade: &Trade) {
        if trade.buyer_id == self.trader_id {
            let position = self.positions.entry(trade.symbol.clone()).or_insert(0);
            *position += trade.quantity;
            self.cash -= trade.price * trade.quantity as f64;
        } else if trade.seller_id == self.trader_id {
            let position = self.positions.entry(trade.symbol.clone()).or_insert(0);
            *position -= trade.quantity;
            self.cash += trade.price * trade.quantity as f64;
        }
        
        self.trade_history.push_back(trade.clone());
        if self.trade_history.len() > 100 {
            self.trade_history.pop_front();
        }
    }

    fn calculate_equity(&self, market_price: f64) -> f64 {
        let position_value: f64 = self.positions.values()
            .map(|&qty| qty as f64 * market_price)
            .sum();
        
        self.cash + position_value
    }

    fn get_position_report(&self, market_price: f64, timestamp: u64) -> PositionReport {
        let unrealized_pnl: f64 = self.positions.values()
            .map(|&qty| qty as f64 * market_price)
            .sum();
        
        PositionReport {
            trader_id: self.trader_id.clone(),
            cash: self.cash,
            positions: self.positions.clone(),
            realized_pnl: self.realized_pnl,
            unrealized_pnl,
            total_equity: self.calculate_equity(market_price),
            timestamp,
        }
    }
}

// ============================================================================
// Trader 2: Mean Reversion Market Maker
// ============================================================================

struct MeanReversionTrader {
    trader_id: String,
    cash: f64,
    positions: HashMap<String, i64>,
    realized_pnl: f64,
    price_history: VecDeque<(u64, f64)>,
    trade_history: VecDeque<Trade>,
    order_counter: u64,
    vwap: f64,
    vwap_volume: i64,
}

impl MeanReversionTrader {
    fn new(trader_id: String, initial_cash: f64) -> Self {
        Self {
            trader_id,
            cash: initial_cash,
            positions: HashMap::new(),
            realized_pnl: 0.0,
            price_history: VecDeque::new(),
            trade_history: VecDeque::new(),
            order_counter: 0,
            vwap: 0.0,
            vwap_volume: 0,
        }
    }

    fn update_market(&mut self, update: &MarketUpdate) {
        self.price_history.push_back((update.timestamp, update.last_price));
        if self.price_history.len() > 100 {
            self.price_history.pop_front();
        }

        // Update VWAP
        if update.volume > 0 {
            self.vwap = (self.vwap * self.vwap_volume as f64 + update.last_price * update.volume as f64)
                / (self.vwap_volume + update.volume) as f64;
            self.vwap_volume += update.volume;
        }
    }

    fn calculate_deviation_from_mean(&self, current_price: f64) -> f64 {
        if self.price_history.len() < 20 {
            return 0.0;
        }

        let recent: Vec<f64> = self.price_history.iter().rev().take(50).map(|(_, p)| *p).collect();
        let mean = recent.iter().sum::<f64>() / recent.len() as f64;

        (current_price - mean) / mean
    }

    fn generate_quote(&mut self, market: &MarketUpdate, timestamp: u64) -> Quote {
        let deviation = self.calculate_deviation_from_mean(market.last_price);
        let position = self.positions.get(&market.symbol).copied().unwrap_or(0);
        
        // Tight spread for market making
        let base_spread = 0.04;
        
        // Skew quotes to fade extremes
        let fade_adjustment = deviation * 0.30;
        
        // Inventory risk adjustment
        let inventory_skew = (position as f64 / 1000.0) * 0.03;
        
        let mid = market.last_price;
        let bid_price = mid - base_spread / 2.0 - fade_adjustment + inventory_skew;
        let ask_price = mid + base_spread / 2.0 - fade_adjustment + inventory_skew;
        
        // Size based on edge and inventory
        let edge = deviation.abs();
        let base_size = 150;
        let size = (base_size as f64 * (1.0 + edge * 3.0)) as i64;
        
        // Reduce size if large inventory
        let inventory_reduction = 1.0 - (position.abs() as f64 / 2000.0).min(0.5);
        let adjusted_size = (size as f64 * inventory_reduction) as i64;
        
        Quote {
            trader_id: self.trader_id.clone(),
            symbol: market.symbol.clone(),
            bid_price: (bid_price * 100.0).round() / 100.0,
            bid_size: adjusted_size,
            ask_price: (ask_price * 100.0).round() / 100.0,
            ask_size: adjusted_size,
            timestamp,
        }
    }

    fn generate_order(&mut self, market: &MarketUpdate, timestamp: u64) -> Option<Order> {
        let deviation = self.calculate_deviation_from_mean(market.last_price);
        let position = self.positions.get(&market.symbol).copied().unwrap_or(0);
        
        // Trade on extreme deviations to mean revert (lowered threshold for more action)
        if deviation.abs() > 0.010 && position.abs() < 1500 {
            let size = 200;
            
            let required_cash = size as f64 * market.last_price;
            if self.cash < required_cash * 0.3 {
                return None;
            }
            
            self.order_counter += 1;
            
            if deviation > 0.0 && position > -1500 {
                // Price too high, sell to fade
                Some(Order {
                    order_id: format!("{}-O{}", self.trader_id, self.order_counter),
                    trader_id: self.trader_id.clone(),
                    symbol: market.symbol.clone(),
                    side: Side::Sell,
                    order_type: OrderType::Limit,
                    price: market.last_price + 0.01,
                    quantity: size,
                    timestamp,
                })
            } else if deviation < 0.0 && position < 1500 {
                // Price too low, buy to fade
                Some(Order {
                    order_id: format!("{}-O{}", self.trader_id, self.order_counter),
                    trader_id: self.trader_id.clone(),
                    symbol: market.symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::Limit,
                    price: market.last_price - 0.01,
                    quantity: size,
                    timestamp,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn process_trade(&mut self, trade: &Trade) {
        if trade.buyer_id == self.trader_id {
            let position = self.positions.entry(trade.symbol.clone()).or_insert(0);
            *position += trade.quantity;
            self.cash -= trade.price * trade.quantity as f64;
        } else if trade.seller_id == self.trader_id {
            let position = self.positions.entry(trade.symbol.clone()).or_insert(0);
            *position -= trade.quantity;
            self.cash += trade.price * trade.quantity as f64;
        }
        
        self.trade_history.push_back(trade.clone());
        if self.trade_history.len() > 100 {
            self.trade_history.pop_front();
        }
    }

    fn calculate_equity(&self, market_price: f64) -> f64 {
        let position_value: f64 = self.positions.values()
            .map(|&qty| qty as f64 * market_price)
            .sum();
        
        self.cash + position_value
    }

    fn get_position_report(&self, market_price: f64, timestamp: u64) -> PositionReport {
        let unrealized_pnl: f64 = self.positions.values()
            .map(|&qty| qty as f64 * market_price)
            .sum();
        
        PositionReport {
            trader_id: self.trader_id.clone(),
            cash: self.cash,
            positions: self.positions.clone(),
            realized_pnl: self.realized_pnl,
            unrealized_pnl,
            total_equity: self.calculate_equity(market_price),
            timestamp,
        }
    }
}

// ============================================================================
// Trader 3: Market Maker (Liquidity Provider)
// ============================================================================

struct MarketMaker {
    trader_id: String,
    cash: f64,
    positions: HashMap<String, i64>,
    realized_pnl: f64,
    trade_history: VecDeque<Trade>,
    order_counter: u64,
    spread_captured: f64,  // Total spread profits
    inventory_target: i64,  // Target inventory (usually 0)
}

impl MarketMaker {
    fn new(trader_id: String, initial_cash: f64) -> Self {
        Self {
            trader_id,
            cash: initial_cash,
            positions: HashMap::new(),
            realized_pnl: 0.0,
            trade_history: VecDeque::new(),
            order_counter: 0,
            spread_captured: 0.0,
            inventory_target: 0,
        }
    }

    fn generate_quote(&mut self, market: &MarketUpdate, timestamp: u64) -> Quote {
        let position = self.positions.get(&market.symbol).copied().unwrap_or(0);
        
        // Very tight spread for market making (0.01 - 0.02)
        let base_spread = 0.015;
        
        // Inventory risk management - skew quotes to reduce position
        // If long, push bid down and ask down to encourage selling
        // If short, push bid up and ask up to encourage buying
        let inventory_skew = (position as f64 / 500.0) * 0.01;
        
        let mid = market.last_price;
        let bid_price = mid - base_spread / 2.0 - inventory_skew;
        let ask_price = mid + base_spread / 2.0 - inventory_skew;
        
        // Size based on how far we are from target inventory
        let inventory_distance = (position - self.inventory_target).abs();
        let base_size = 300;
        
        // Reduce size if large inventory to manage risk
        let size_multiplier = 1.0 - (inventory_distance as f64 / 2000.0).min(0.5);
        let size = (base_size as f64 * size_multiplier) as i64;
        
        Quote {
            trader_id: self.trader_id.clone(),
            symbol: market.symbol.clone(),
            bid_price: (bid_price * 100.0).round() / 100.0,
            bid_size: size.max(50),
            ask_price: (ask_price * 100.0).round() / 100.0,
            ask_size: size.max(50),
            timestamp,
        }
    }

    fn generate_order(&mut self, market: &MarketUpdate, timestamp: u64) -> Option<Order> {
        let position = self.positions.get(&market.symbol).copied().unwrap_or(0);
        
        // Aggressively flatten inventory if too large
        if position.abs() > 800 {
            self.order_counter += 1;
            
            let flatten_size = (position.abs() / 4).max(100); // Flatten 25% at a time
            
            if position > 0 {
                // Long position - sell to flatten
                Some(Order {
                    order_id: format!("{}-O{}", self.trader_id, self.order_counter),
                    trader_id: self.trader_id.clone(),
                    symbol: market.symbol.clone(),
                    side: Side::Sell,
                    order_type: OrderType::IOC,
                    price: market.bid_price - 0.01, // Aggressive
                    quantity: flatten_size,
                    timestamp,
                })
            } else {
                // Short position - buy to flatten
                Some(Order {
                    order_id: format!("{}-O{}", self.trader_id, self.order_counter),
                    trader_id: self.trader_id.clone(),
                    symbol: market.symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::IOC,
                    price: market.ask_price + 0.01, // Aggressive
                    quantity: flatten_size,
                    timestamp,
                })
            }
        } else {
            None
        }
    }

    fn process_trade(&mut self, trade: &Trade) {
        if trade.buyer_id == self.trader_id {
            let position = self.positions.entry(trade.symbol.clone()).or_insert(0);
            *position += trade.quantity;
            self.cash -= trade.price * trade.quantity as f64;
            
            // Track spread capture (we bought, hopefully at bid)
            // Simplified: assume we're providing liquidity
        } else if trade.seller_id == self.trader_id {
            let position = self.positions.entry(trade.symbol.clone()).or_insert(0);
            *position -= trade.quantity;
            self.cash += trade.price * trade.quantity as f64;
            
            // Track spread capture (we sold, hopefully at ask)
        }
        
        self.trade_history.push_back(trade.clone());
        if self.trade_history.len() > 100 {
            self.trade_history.pop_front();
        }
    }

    fn calculate_equity(&self, market_price: f64) -> f64 {
        let position_value: f64 = self.positions.values()
            .map(|&qty| qty as f64 * market_price)
            .sum();
        
        self.cash + position_value
    }

    fn get_position_report(&self, market_price: f64, timestamp: u64) -> PositionReport {
        let unrealized_pnl: f64 = self.positions.values()
            .map(|&qty| qty as f64 * market_price)
            .sum();
        
        PositionReport {
            trader_id: self.trader_id.clone(),
            cash: self.cash,
            positions: self.positions.clone(),
            realized_pnl: self.realized_pnl,
            unrealized_pnl,
            total_equity: self.calculate_equity(market_price),
            timestamp,
        }
    }
}

// ============================================================================
// Main Competition
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🎯 Starting Three-Trader Competition (with Market Maker)");
    info!("═══════════════════════════════════════════════════════════");

    // Initialize DOLDA mailbox system
    let temp_dir = tempfile::tempdir()?;
    let data_path = temp_dir.path().join("competition");
    std::fs::create_dir_all(&data_path)?;

    let mut system = MailboxSystem::new(data_path.to_string_lossy().to_string()).await?;

    // Create mailboxes for communication
    let quotes_mb = system.create_mailbox("quotes@competition").await?;
    let _orders_mb = system.create_mailbox("orders@competition").await?;
    let trades_mb = system.create_mailbox("trades@competition").await?;

    // Initialize traders
    let initial_capital = 1_000_000.0;
    let loss_threshold = 0.80; // 20% loss

    let mut trader1 = MomentumTrader::new("MOMENTUM-1".to_string(), initial_capital);
    let mut trader2 = MeanReversionTrader::new("MEANREV-2".to_string(), initial_capital);
    let mut market_maker = MarketMaker::new("MARKETMKR-3".to_string(), initial_capital);

    // Initialize order book
    let symbol = "COMP".to_string();
    let initial_price = 100.0;
    let mut order_book = OrderBook::new(symbol.clone(), initial_price);

    info!("💰 Initial Capital: ${:.2} each", initial_capital);
    info!("🎲 Symbol: {}", symbol);
    info!("💲 Initial Price: ${:.2}", initial_price);
    info!("🛑 Stop Condition: Equity < ${:.2} (20% loss) - for competing traders only", initial_capital * loss_threshold);
    info!("📊 Traders: MOMENTUM-1 (trend), MEANREV-2 (revert), MARKETMKR-3 (liquidity)");
    info!("");

    let start_time = Instant::now();
    let mut iteration = 0;
    let mut last_report_time = Instant::now();
    let mut rng = rand::thread_rng();

    loop {
        iteration += 1;
        let timestamp = start_time.elapsed().as_millis() as u64;

        // Add market noise to simulate external price movement
        if iteration % 10 == 0 {
            use rand::Rng;
            let noise = rng.gen_range(-0.05..0.05);
            order_book.last_trade_price = (order_book.last_trade_price * (1.0 + noise)).max(50.0).min(150.0);
        }

        // Generate market update
        let market_update = order_book.get_market_update(timestamp);

        // Update traders with market data
        trader1.update_market(&market_update);
        trader2.update_market(&market_update);
        // MM doesn't need market history, just current prices

        // Generate quotes (MM quotes every iteration for tight markets)
        let quote1 = trader1.generate_quote(&market_update, timestamp);
        let quote2 = trader2.generate_quote(&market_update, timestamp);
        let quote_mm = market_maker.generate_quote(&market_update, timestamp);

        // Update order book with quotes
        order_book.update_quote(quote1.clone());
        order_book.update_quote(quote2.clone());
        order_book.update_quote(quote_mm.clone());

        // Send quotes to mailbox (logging only)
        let quote1_data = bincode::serialize(&MessageType::Quote(quote1))?;
        let quote2_data = bincode::serialize(&MessageType::Quote(quote2))?;
        let quote_mm_data = bincode::serialize(&MessageType::Quote(quote_mm))?;
        let _ = quotes_mb.send_to("quotes@competition", &quote1_data).await;
        let _ = quotes_mb.send_to("quotes@competition", &quote2_data).await;
        let _ = quotes_mb.send_to("quotes@competition", &quote_mm_data).await;

        // Generate orders (more frequently for active competition)
        if iteration % 3 == 0 {
            if let Some(order) = trader1.generate_order(&market_update, timestamp) {
                let trades = order_book.match_order(order.clone());
                
                for trade in &trades {
                    trader1.process_trade(trade);
                    trader2.process_trade(trade);
                    market_maker.process_trade(trade);
                    
                    let trade_data = bincode::serialize(&MessageType::Trade(trade.clone()))?;
                    let _ = trades_mb.send_to("trades@competition", &trade_data).await;
                }
            }

            if let Some(order) = trader2.generate_order(&market_update, timestamp) {
                let trades = order_book.match_order(order.clone());
                
                for trade in &trades {
                    trader1.process_trade(trade);
                    trader2.process_trade(trade);
                    market_maker.process_trade(trade);
                    
                    let trade_data = bincode::serialize(&MessageType::Trade(trade.clone()))?;
                    let _ = trades_mb.send_to("trades@competition", &trade_data).await;
                }
            }
            
            // MM tries to flatten inventory
            if let Some(order) = market_maker.generate_order(&market_update, timestamp) {
                let trades = order_book.match_order(order.clone());
                
                for trade in &trades {
                    trader1.process_trade(trade);
                    trader2.process_trade(trade);
                    market_maker.process_trade(trade);
                    
                    let trade_data = bincode::serialize(&MessageType::Trade(trade.clone()))?;
                    let _ = trades_mb.send_to("trades@competition", &trade_data).await;
                }
            }
        }

        // Calculate equities
        let market_price = order_book.last_trade_price;
        let equity1 = trader1.calculate_equity(market_price);
        let equity2 = trader2.calculate_equity(market_price);
        let equity_mm = market_maker.calculate_equity(market_price);

        // Check stop condition (only for competing traders, not MM)
        if equity1 < initial_capital * loss_threshold || equity2 < initial_capital * loss_threshold {
            info!("");
            info!("🛑 STOP CONDITION REACHED!");
            info!("═══════════════════════════════════");
            
            let report1 = trader1.get_position_report(market_price, timestamp);
            let report2 = trader2.get_position_report(market_price, timestamp);
            let report_mm = market_maker.get_position_report(market_price, timestamp);
            
            info!("📊 Final Results:");
            info!("");
            info!("  {} (Momentum)", trader1.trader_id);
            info!("    💰 Total Equity: ${:.2}", report1.total_equity);
            info!("    💵 Cash: ${:.2}", report1.cash);
            info!("    📦 Position: {:?}", report1.positions);
            info!("    📈 P&L: ${:.2}", report1.total_equity - initial_capital);
            info!("    📉 Return: {:.2}%", (report1.total_equity / initial_capital - 1.0) * 100.0);
            info!("");
            info!("  {} (Mean Reversion)", trader2.trader_id);
            info!("    💰 Total Equity: ${:.2}", report2.total_equity);
            info!("    💵 Cash: ${:.2}", report2.cash);
            info!("    📦 Position: {:?}", report2.positions);
            info!("    📈 P&L: ${:.2}", report2.total_equity - initial_capital);
            info!("    📉 Return: {:.2}%", (report2.total_equity / initial_capital - 1.0) * 100.0);
            info!("");
            info!("  {} (Market Maker)", market_maker.trader_id);
            info!("    💰 Total Equity: ${:.2}", report_mm.total_equity);
            info!("    💵 Cash: ${:.2}", report_mm.cash);
            info!("    📦 Position: {:?}", report_mm.positions);
            info!("    📈 P&L: ${:.2}", report_mm.total_equity - initial_capital);
            info!("    📉 Return: {:.2}%", (report_mm.total_equity / initial_capital - 1.0) * 100.0);
            info!("    💸 Spread Captured: ${:.2}", market_maker.spread_captured);
            info!("");
            
            let winner = if equity1 > equity2 { &trader1.trader_id } else { &trader2.trader_id };
            info!("🏆 Winner (competing traders): {}", winner);
            info!("💹 Market Maker P&L: ${:.2} ({:.2}%)", 
                  report_mm.total_equity - initial_capital,
                  (report_mm.total_equity / initial_capital - 1.0) * 100.0);
            info!("⏱️  Duration: {:.2}s", start_time.elapsed().as_secs_f64());
            info!("🔄 Iterations: {}", iteration);
            info!("💱 Total Trades: {}", order_book.trades.len());
            info!("💲 Final Price: ${:.2}", market_price);
            
            break;
        }

        // Periodic reporting
        if last_report_time.elapsed() > Duration::from_secs(5) {
            info!("📊 Iteration {} | Price: ${:.2} | Spread: ${:.4}", 
                  iteration, market_price, order_book.get_spread());
            info!("   {} Equity: ${:.2} | Pos: {:?}", 
                  trader1.trader_id, equity1, trader1.positions.get(&symbol).unwrap_or(&0));
            info!("   {} Equity: ${:.2} | Pos: {:?}", 
                  trader2.trader_id, equity2, trader2.positions.get(&symbol).unwrap_or(&0));
            info!("   {} Equity: ${:.2} | Pos: {:?} | Trades: {}", 
                  market_maker.trader_id, equity_mm, 
                  market_maker.positions.get(&symbol).unwrap_or(&0),
                  market_maker.trade_history.len());
            info!("");
            
            last_report_time = Instant::now();
        }

        // Small delay to make simulation observable but fast
        sleep(Duration::from_millis(2)).await;
    }

    info!("✅ Competition complete!");
    Ok(())
}

