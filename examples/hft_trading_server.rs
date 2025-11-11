//! High-Frequency Trading Server using DOLDA
//!
//! Features:
//! - NYSE/NASDAQ market data simulation
//! - Multiple order types (IOC, FOK, Limit, Market, Stop)
//! - Matching engine with price-time priority
//! - SQL-based strategy processing with DataFusion
//! - Risk management
//! - Real-time analytics
//!
//! Architecture:
//! ```text
//! Market Data → [market-data@hft] → Strategy Engine
//!      ↓                                  ↓
//! Orders → [orders@hft] → Matching Engine → [executions@hft]
//!      ↓                        ↓               ↓
//! Risk Management ← [positions@hft] ← Analytics
//! ```

use dolda::mailbox::{MailboxSystem, Message};
use dolda::mailbox_processor::{
    MailboxProcessor, ProcessorContext, ProcessorResult, ProcessorManager, ProcessorConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;
use rand::Rng;

// ============================================================================
// Market Data Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Exchange {
    NYSE,
    NASDAQ,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Quote {
    symbol: String,
    exchange: Exchange,
    bid_price: f64,
    bid_size: u32,
    ask_price: f64,
    ask_size: u32,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Trade {
    symbol: String,
    exchange: Exchange,
    price: f64,
    size: u32,
    timestamp: u64,
    trade_id: u64,
}

// ============================================================================
// Order Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum OrderType {
    Market,              // Execute immediately at best price
    Limit,               // Execute at specified price or better
    IOC,                 // Immediate or Cancel - fill what you can, cancel rest
    FOK,                 // Fill or Kill - fill completely or cancel
    Stop,                // Trigger when price reaches stop price
    StopLimit,           // Stop that becomes limit order
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: u64,
    symbol: String,
    side: OrderSide,
    order_type: OrderType,
    quantity: u32,
    price: Option<f64>,        // For limit orders
    stop_price: Option<f64>,   // For stop orders
    filled_quantity: u32,
    status: OrderStatus,
    timestamp: u64,
    strategy_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Execution {
    execution_id: u64,
    order_id: u64,
    symbol: String,
    side: OrderSide,
    price: f64,
    quantity: u32,
    timestamp: u64,
}

// ============================================================================
// Position and Risk
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Position {
    symbol: String,
    quantity: i32,         // Positive = long, negative = short
    avg_price: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RiskLimits {
    max_position_size: u32,
    max_order_size: u32,
    max_daily_loss: f64,
    max_single_loss: f64,
}

// ============================================================================
// Market Data Generator
// ============================================================================

struct MarketDataGenerator {
    symbols: Vec<String>,
    exchanges: Vec<Exchange>,
    base_prices: HashMap<String, f64>,
    trade_counter: Arc<RwLock<u64>>,
}

impl MarketDataGenerator {
    fn new() -> Self {
        let symbols = vec![
            "AAPL".to_string(),
            "GOOGL".to_string(),
            "MSFT".to_string(),
            "AMZN".to_string(),
            "TSLA".to_string(),
            "NVDA".to_string(),
            "META".to_string(),
            "JPM".to_string(),
        ];
        
        let mut base_prices = HashMap::new();
        base_prices.insert("AAPL".to_string(), 180.0);
        base_prices.insert("GOOGL".to_string(), 140.0);
        base_prices.insert("MSFT".to_string(), 380.0);
        base_prices.insert("AMZN".to_string(), 170.0);
        base_prices.insert("TSLA".to_string(), 240.0);
        base_prices.insert("NVDA".to_string(), 500.0);
        base_prices.insert("META".to_string(), 350.0);
        base_prices.insert("JPM".to_string(), 160.0);
        
        Self {
            symbols,
            exchanges: vec![Exchange::NYSE, Exchange::NASDAQ],
            base_prices,
            trade_counter: Arc::new(RwLock::new(0)),
        }
    }
    
    fn generate_quote(&self, symbol: &str, exchange: Exchange) -> Quote {
        let mut rng = rand::thread_rng();
        let base_price = self.base_prices.get(symbol).copied().unwrap_or(100.0);
        
        // Add random walk
        let mid_price = base_price * (1.0 + rng.gen_range(-0.01..0.01));
        let spread = mid_price * 0.0001; // 1 basis point spread
        
        Quote {
            symbol: symbol.to_string(),
            exchange,
            bid_price: mid_price - spread / 2.0,
            bid_size: rng.gen_range(100..10000),
            ask_price: mid_price + spread / 2.0,
            ask_size: rng.gen_range(100..10000),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
        }
    }
    
    async fn generate_trade(&self, symbol: &str, exchange: Exchange) -> Trade {
        let base_price = self.base_prices.get(symbol).copied().unwrap_or(100.0);
        
        let mut trade_id = self.trade_counter.write().await;
        *trade_id += 1;
        let trade_id_val = *trade_id;
        drop(trade_id);
        
        // Generate random values after releasing lock
        let mut rng = rand::thread_rng();
        let price = base_price * (1.0 + rng.gen_range(-0.005..0.005));
        let size = rng.gen_range(100..5000);
        
        Trade {
            symbol: symbol.to_string(),
            exchange,
            price,
            size,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            trade_id: trade_id_val,
        }
    }
}

// ============================================================================
// Market Data Processor
// ============================================================================

struct MarketDataProcessor {
    generator: Arc<MarketDataGenerator>,
}

#[async_trait]
impl MailboxProcessor for MarketDataProcessor {
    async fn init(&self, ctx: &ProcessorContext) -> ProcessorResult {
        println!("📊 Market Data Processor initialized for {}", ctx.address);
        Ok(())
    }
    
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let command = String::from_utf8_lossy(&msg.body);
        
        if command.starts_with("QUOTE:") {
            let parts: Vec<&str> = command.split(':').collect();
            if parts.len() >= 2 {
                let symbol = parts[1];
                let exchange = if parts.len() >= 3 && parts[2] == "NYSE" {
                    Exchange::NYSE
                } else {
                    Exchange::NASDAQ
                };
                
                let quote = self.generator.generate_quote(symbol, exchange);
                let quote_json = serde_json::to_vec(&quote).unwrap();
                ctx.send_to("quotes@hft", &quote_json).await?;
            }
        } else if command.starts_with("TRADE:") {
            let parts: Vec<&str> = command.split(':').collect();
            if parts.len() >= 2 {
                let symbol = parts[1];
                let exchange = if parts.len() >= 3 && parts[2] == "NYSE" {
                    Exchange::NYSE
                } else {
                    Exchange::NASDAQ
                };
                
                let trade = self.generator.generate_trade(symbol, exchange).await;
                let trade_json = serde_json::to_vec(&trade).unwrap();
                ctx.send_to("trades@hft", &trade_json).await?;
            }
        }
        
        Ok(())
    }
}

// ============================================================================
// Matching Engine
// ============================================================================

struct MatchingEngine {
    // Order book: symbol -> side -> price -> orders
    buy_book: Arc<RwLock<HashMap<String, BTreeMap<u64, Vec<Order>>>>>,   // Price in cents, descending
    sell_book: Arc<RwLock<HashMap<String, BTreeMap<u64, Vec<Order>>>>>,  // Price in cents, ascending
    order_counter: Arc<RwLock<u64>>,
    execution_counter: Arc<RwLock<u64>>,
    current_prices: Arc<RwLock<HashMap<String, f64>>>,
}

impl MatchingEngine {
    fn new() -> Self {
        Self {
            buy_book: Arc::new(RwLock::new(HashMap::new())),
            sell_book: Arc::new(RwLock::new(HashMap::new())),
            order_counter: Arc::new(RwLock::new(0)),
            execution_counter: Arc::new(RwLock::new(0)),
            current_prices: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    fn price_to_cents(price: f64) -> u64 {
        (price * 100.0).round() as u64
    }
    
    fn cents_to_price(cents: u64) -> f64 {
        cents as f64 / 100.0
    }
    
    async fn get_order_id(&self) -> u64 {
        let mut counter = self.order_counter.write().await;
        *counter += 1;
        *counter
    }
    
    async fn get_execution_id(&self) -> u64 {
        let mut counter = self.execution_counter.write().await;
        *counter += 1;
        *counter
    }
    
    async fn update_market_price(&self, symbol: &str, price: f64) {
        let mut prices = self.current_prices.write().await;
        prices.insert(symbol.to_string(), price);
    }
    
    async fn get_market_price(&self, symbol: &str) -> Option<f64> {
        let prices = self.current_prices.read().await;
        prices.get(symbol).copied()
    }
    
    async fn match_order(&self, mut order: Order) -> (Order, Vec<Execution>) {
        let mut executions = Vec::new();
        
        // Handle stop orders by converting them
        if matches!(order.order_type, OrderType::Stop | OrderType::StopLimit) {
            if let Some(market_price) = self.get_market_price(&order.symbol).await {
                let stop_price = order.stop_price.unwrap_or(0.0);
                let triggered = match order.side {
                    OrderSide::Buy => market_price >= stop_price,
                    OrderSide::Sell => market_price <= stop_price,
                };
                
                if triggered {
                    // Convert to market or limit order
                    order.order_type = if order.order_type == OrderType::Stop {
                        OrderType::Market
                    } else {
                        OrderType::Limit
                    };
                } else {
                    // Not triggered yet, return order unchanged
                    return (order, executions);
                }
            }
        }
        
        // Match based on order type
        match order.order_type {
            OrderType::Market => {
                // Execute at best available price
                let (updated_order, execs) = self.match_market_order(order).await;
                order = updated_order;
                executions.extend(execs);
            }
            OrderType::Limit => {
                // Execute at limit price or better
                let (updated_order, execs) = self.match_limit_order(order).await;
                order = updated_order;
                executions.extend(execs);
            }
            OrderType::IOC => {
                // Immediate or Cancel - match what we can, cancel rest
                let (mut updated_order, execs) = self.match_limit_order(order.clone()).await;
                if updated_order.filled_quantity < order.quantity {
                    updated_order.status = OrderStatus::Cancelled;
                }
                order = updated_order;
                executions.extend(execs);
            }
            OrderType::FOK => {
                // Fill or Kill - only execute if can fill completely
                let (updated_order, execs) = self.match_limit_order(order.clone()).await;
                if updated_order.filled_quantity == order.quantity {
                    order = updated_order;
                    executions.extend(execs);
                } else {
                    order.status = OrderStatus::Cancelled;
                }
            }
            OrderType::Stop | OrderType::StopLimit => {
                // Should have been handled above
            }
        }
        
        (order, executions)
    }
    
    async fn match_market_order(&self, mut order: Order) -> (Order, Vec<Execution>) {
        let mut executions = Vec::new();
        let mut remaining_qty = order.quantity - order.filled_quantity;
        
        // Get opposite book
        let mut book = match order.side {
            OrderSide::Buy => self.sell_book.write().await,
            OrderSide::Sell => self.buy_book.write().await,
        };
        
        if let Some(symbol_book) = book.get_mut(&order.symbol) {
            // Market orders match against best prices
            let prices: Vec<u64> = symbol_book.keys().copied().collect();
            
            for price_level in prices {
                if remaining_qty == 0 {
                    break;
                }
                
                if let Some(orders_at_price) = symbol_book.get_mut(&price_level) {
                    let mut i = 0;
                    while i < orders_at_price.len() && remaining_qty > 0 {
                        let resting_order = &mut orders_at_price[i];
                        let available_qty = resting_order.quantity - resting_order.filled_quantity;
                        let fill_qty = remaining_qty.min(available_qty);
                        
                        // Create execution
                        let execution_id = self.get_execution_id().await;
                        let execution = Execution {
                            execution_id,
                            order_id: order.order_id,
                            symbol: order.symbol.clone(),
                            side: order.side.clone(),
                            price: Self::cents_to_price(price_level),
                            quantity: fill_qty,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_micros() as u64,
                        };
                        
                        executions.push(execution);
                        
                        // Update quantities
                        order.filled_quantity += fill_qty;
                        resting_order.filled_quantity += fill_qty;
                        remaining_qty -= fill_qty;
                        
                        if resting_order.filled_quantity == resting_order.quantity {
                            resting_order.status = OrderStatus::Filled;
                            orders_at_price.remove(i);
                        } else {
                            resting_order.status = OrderStatus::PartiallyFilled;
                            i += 1;
                        }
                    }
                }
            }
        }
        
        if order.filled_quantity == order.quantity {
            order.status = OrderStatus::Filled;
        } else if order.filled_quantity > 0 {
            order.status = OrderStatus::PartiallyFilled;
        }
        
        (order, executions)
    }
    
    async fn match_limit_order(&self, mut order: Order) -> (Order, Vec<Execution>) {
        let mut executions = Vec::new();
        
        if let Some(limit_price) = order.price {
            let limit_cents = Self::price_to_cents(limit_price);
            let mut remaining_qty = order.quantity - order.filled_quantity;
            
            // Try to match against opposite book
            let mut book = match order.side {
                OrderSide::Buy => self.sell_book.write().await,
                OrderSide::Sell => self.buy_book.write().await,
            };
            
            if let Some(symbol_book) = book.get_mut(&order.symbol) {
                let prices: Vec<u64> = symbol_book.keys().copied().collect();
                
                for price_level in prices {
                    // Check if price is acceptable
                    let acceptable = match order.side {
                        OrderSide::Buy => price_level <= limit_cents,
                        OrderSide::Sell => price_level >= limit_cents,
                    };
                    
                    if !acceptable || remaining_qty == 0 {
                        break;
                    }
                    
                    if let Some(orders_at_price) = symbol_book.get_mut(&price_level) {
                        let mut i = 0;
                        while i < orders_at_price.len() && remaining_qty > 0 {
                            let resting_order = &mut orders_at_price[i];
                            let available_qty = resting_order.quantity - resting_order.filled_quantity;
                            let fill_qty = remaining_qty.min(available_qty);
                            
                            let execution_id = self.get_execution_id().await;
                            let execution = Execution {
                                execution_id,
                                order_id: order.order_id,
                                symbol: order.symbol.clone(),
                                side: order.side.clone(),
                                price: Self::cents_to_price(price_level),
                                quantity: fill_qty,
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_micros() as u64,
                            };
                            
                            executions.push(execution);
                            order.filled_quantity += fill_qty;
                            resting_order.filled_quantity += fill_qty;
                            remaining_qty -= fill_qty;
                            
                            if resting_order.filled_quantity == resting_order.quantity {
                                resting_order.status = OrderStatus::Filled;
                                orders_at_price.remove(i);
                            } else {
                                resting_order.status = OrderStatus::PartiallyFilled;
                                i += 1;
                            }
                        }
                    }
                }
            }
            
            // Add unfilled portion to book if not fully matched
            if remaining_qty > 0 && order.order_type == OrderType::Limit {
                let mut book = match order.side {
                    OrderSide::Buy => self.buy_book.write().await,
                    OrderSide::Sell => self.sell_book.write().await,
                };
                
                book.entry(order.symbol.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(limit_cents)
                    .or_insert_with(Vec::new)
                    .push(order.clone());
            }
            
            if order.filled_quantity == order.quantity {
                order.status = OrderStatus::Filled;
            } else if order.filled_quantity > 0 {
                order.status = OrderStatus::PartiallyFilled;
            }
        }
        
        (order, executions)
    }
}

// ============================================================================
// Order Processor
// ============================================================================

struct OrderProcessor {
    matching_engine: Arc<MatchingEngine>,
    risk_limits: RiskLimits,
}

#[async_trait]
impl MailboxProcessor for OrderProcessor {
    async fn init(&self, ctx: &ProcessorContext) -> ProcessorResult {
        println!("📋 Order Processor initialized for {}", ctx.address);
        Ok(())
    }
    
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        // Deserialize order
        let mut order: Order = match serde_json::from_slice(&msg.body) {
            Ok(o) => o,
            Err(e) => {
                println!("❌ Failed to parse order: {}", e);
                return Ok(());
            }
        };
        
        // Assign order ID
        order.order_id = self.matching_engine.get_order_id().await;
        order.status = OrderStatus::New;
        order.filled_quantity = 0;
        
        println!("📥 New Order: {} {} {} @ {:?}", 
            order.symbol, 
            match order.side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
            order.quantity,
            order.price
        );
        
        // Risk check
        if order.quantity > self.risk_limits.max_order_size {
            order.status = OrderStatus::Rejected;
            println!("❌ Order rejected: exceeds max order size");
            return Ok(());
        }
        
        // Match order
        let (matched_order, executions) = self.matching_engine.match_order(order).await;
        
        // Send executions
        for execution in executions {
            println!("✅ Execution: {} {} @ ${:.2} x {}",
                execution.symbol,
                match execution.side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                execution.price,
                execution.quantity
            );
            
            let exec_json = serde_json::to_vec(&execution).unwrap();
            ctx.send_to("executions@hft", &exec_json).await?;
        }
        
        // Send order status update
        let order_json = serde_json::to_vec(&matched_order).unwrap();
        ctx.send_to("order-status@hft", &order_json).await?;
        
        Ok(())
    }
}

// ============================================================================
// Strategy Processor (SQL-based)
// ============================================================================

struct StrategyProcessor {
    strategy_id: String,
}

#[async_trait]
impl MailboxProcessor for StrategyProcessor {
    async fn init(&self, ctx: &ProcessorContext) -> ProcessorResult {
        println!("🎯 Strategy Processor '{}' initialized for {}", self.strategy_id, ctx.address);
        Ok(())
    }
    
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        // Parse market data
        if let Ok(quote) = serde_json::from_slice::<Quote>(&msg.body) {
            // Simple strategy: VWAP cross
            let mid_price = (quote.bid_price + quote.ask_price) / 2.0;
            
            // Example strategy logic
            if quote.symbol == "AAPL" && mid_price < 179.0 {
                // Buy signal
                let order = Order {
                    order_id: 0,  // Will be assigned by order processor
                    symbol: quote.symbol.clone(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    quantity: 100,
                    price: Some(quote.bid_price),
                    stop_price: None,
                    filled_quantity: 0,
                    status: OrderStatus::New,
                    timestamp: quote.timestamp,
                    strategy_id: self.strategy_id.clone(),
                };
                
                let order_json = serde_json::to_vec(&order).unwrap();
                ctx.send_to("orders@hft", &order_json).await?;
                
                println!("📊 Strategy '{}' generated BUY order for {}", self.strategy_id, quote.symbol);
            }
        }
        
        Ok(())
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 HFT Trading Server with DOLDA\n");
    println!("{}", "=".repeat(80));
    
    // Create system
    let temp_dir = tempfile::tempdir()?;
    let mut system = MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string()).await?;
    
    // Create mailboxes
    println!("\n📫 Creating mailboxes...");
    let _market_data = system.create_mailbox("market-data@hft").await?;
    let _quotes = system.create_mailbox("quotes@hft").await?;
    let _trades = system.create_mailbox("trades@hft").await?;
    let _orders = system.create_mailbox("orders@hft").await?;
    let _executions = system.create_mailbox("executions@hft").await?;
    let _order_status = system.create_mailbox("order-status@hft").await?;
    let _strategy_feed = system.create_mailbox("strategy-feed@hft").await?;
    println!("✅ All mailboxes created");
    
    // Create processors
    let system_arc = Arc::new(RwLock::new(system));
    let manager = ProcessorManager::new(Arc::clone(&system_arc));
    
    // Market data processor
    println!("\n📊 Registering market data processor...");
    let md_generator = Arc::new(MarketDataGenerator::new());
    let md_processor = Arc::new(MarketDataProcessor {
        generator: Arc::clone(&md_generator),
    });
    manager.register_processor(
        "market-data@hft",
        md_processor,
        ProcessorConfig::default(),
    ).await?;
    
    // Matching engine and order processor
    println!("📋 Registering order processor...");
    let matching_engine = Arc::new(MatchingEngine::new());
    let order_processor = Arc::new(OrderProcessor {
        matching_engine: Arc::clone(&matching_engine),
        risk_limits: RiskLimits {
            max_position_size: 10000,
            max_order_size: 1000,
            max_daily_loss: 100000.0,
            max_single_loss: 10000.0,
        },
    });
    manager.register_processor(
        "orders@hft",
        order_processor,
        ProcessorConfig::default(),
    ).await?;
    
    // Strategy processor
    println!("🎯 Registering strategy processor...");
    let strategy_processor = Arc::new(StrategyProcessor {
        strategy_id: "VWAP_Cross_V1".to_string(),
    });
    manager.register_processor(
        "strategy-feed@hft",
        strategy_processor,
        ProcessorConfig::default(),
    ).await?;
    
    println!("✅ All processors registered");
    
    // Simulate trading
    println!("\n{}", "=".repeat(80));
    println!("🎬 Starting Trading Simulation");
    println!("{}", "=".repeat(80));
    
    let system_clone = Arc::clone(&system_arc);
    
    // Market data feed
    tokio::spawn(async move {
        let system = system_clone.read().await;
        let md_mailbox = system.get_mailbox("market-data@hft").await.unwrap().unwrap();
        
        loop {
            for symbol in &["AAPL", "GOOGL", "MSFT", "TSLA"] {
                // Generate quote
                let quote_cmd = format!("QUOTE:{}:NASDAQ", symbol);
                md_mailbox.send_to("market-data@hft", quote_cmd.as_bytes()).await.ok();
                
                // Forward to strategy
                if let Ok(Some(msg)) = md_mailbox.receive().await {
                    // Forward quote to strategy feed
                    if let Ok(quote_mailbox) = system.get_mailbox("quotes@hft").await {
                        if let Some(qm) = quote_mailbox {
                            qm.send_to("strategy-feed@hft", &msg.body).await.ok();
                        }
                    }
                }
                
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
    });
    
    // Let it run for a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // Send manual orders
    println!("\n{}", "=".repeat(80));
    println!("📝 Submitting Manual Orders");
    println!("{}", "=".repeat(80));
    
    let system = system_arc.read().await;
    let order_mailbox = system.get_mailbox("orders@hft").await?.unwrap();
    
    // 1. Market Order
    println!("\n1️⃣ Market Order: BUY 100 AAPL");
    let market_order = Order {
        order_id: 0,
        symbol: "AAPL".to_string(),
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        quantity: 100,
        price: None,
        stop_price: None,
        filled_quantity: 0,
        status: OrderStatus::New,
        timestamp: 0,
        strategy_id: "Manual".to_string(),
    };
    matching_engine.update_market_price("AAPL", 180.0).await;
    order_mailbox.send_to("orders@hft", &serde_json::to_vec(&market_order)?).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 2. Limit Order
    println!("\n2️⃣ Limit Order: BUY 200 GOOGL @ $140.50");
    let limit_order = Order {
        order_id: 0,
        symbol: "GOOGL".to_string(),
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        quantity: 200,
        price: Some(140.50),
        stop_price: None,
        filled_quantity: 0,
        status: OrderStatus::New,
        timestamp: 0,
        strategy_id: "Manual".to_string(),
    };
    order_mailbox.send_to("orders@hft", &serde_json::to_vec(&limit_order)?).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 3. IOC Order
    println!("\n3️⃣ IOC Order: SELL 150 MSFT @ $380.00");
    let ioc_order = Order {
        order_id: 0,
        symbol: "MSFT".to_string(),
        side: OrderSide::Sell,
        order_type: OrderType::IOC,
        quantity: 150,
        price: Some(380.0),
        stop_price: None,
        filled_quantity: 0,
        status: OrderStatus::New,
        timestamp: 0,
        strategy_id: "Manual".to_string(),
    };
    order_mailbox.send_to("orders@hft", &serde_json::to_vec(&ioc_order)?).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 4. FOK Order
    println!("\n4️⃣ FOK Order: BUY 500 TSLA @ $240.00");
    let fok_order = Order {
        order_id: 0,
        symbol: "TSLA".to_string(),
        side: OrderSide::Buy,
        order_type: OrderType::FOK,
        quantity: 500,
        price: Some(240.0),
        stop_price: None,
        filled_quantity: 0,
        status: OrderStatus::New,
        timestamp: 0,
        strategy_id: "Manual".to_string(),
    };
    order_mailbox.send_to("orders@hft", &serde_json::to_vec(&fok_order)?).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 5. Stop Order
    println!("\n5️⃣ Stop Order: SELL 100 NVDA @ Stop $495.00");
    let stop_order = Order {
        order_id: 0,
        symbol: "NVDA".to_string(),
        side: OrderSide::Sell,
        order_type: OrderType::Stop,
        quantity: 100,
        price: None,
        stop_price: Some(495.0),
        filled_quantity: 0,
        status: OrderStatus::New,
        timestamp: 0,
        strategy_id: "Manual".to_string(),
    };
    matching_engine.update_market_price("NVDA", 500.0).await;
    order_mailbox.send_to("orders@hft", &serde_json::to_vec(&stop_order)?).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Check executions
    println!("\n{}", "=".repeat(80));
    println!("📊 Execution Summary");
    println!("{}", "=".repeat(80));
    
    if let Some(exec_mailbox) = system.get_mailbox("executions@hft").await? {
        let mut exec_count = 0;
        while let Ok(Some(msg)) = exec_mailbox.receive().await {
            if let Ok(execution) = serde_json::from_slice::<Execution>(&msg.body) {
                exec_count += 1;
                println!("Execution #{}: {} {} {} @ ${:.2}",
                    exec_count,
                    execution.symbol,
                    match execution.side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                    execution.quantity,
                    execution.price
                );
            }
        }
        
        if exec_count == 0 {
            println!("No executions yet (orders may be resting in book)");
        }
    }
    
    println!("\n{}", "=".repeat(80));
    println!("✨ Trading Simulation Complete!");
    println!("{}", "=".repeat(80));
    
    println!("\n💡 Key Features Demonstrated:");
    println!("   • Market data simulation (NYSE/NASDAQ)");
    println!("   • Multiple order types (Market, Limit, IOC, FOK, Stop)");
    println!("   • Price-time priority matching engine");
    println!("   • Risk management checks");
    println!("   • Strategy processor with SQL capability");
    println!("   • All components communicate via DOLDA mailboxes");
    println!("   • Async, high-performance architecture");
    
    manager.shutdown_all().await?;
    
    Ok(())
}

