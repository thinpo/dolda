//! Error recovery and resilience patterns
//!
//! This module provides:
//! - Automatic retry logic with exponential backoff
//! - Circuit breaker pattern for failing operations
//! - Graceful degradation strategies
//! - Corruption detection and recovery

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ResilienceError {
    #[error("Operation failed after {0} retries")]
    MaxRetriesExceeded(usize),
    
    #[error("Circuit breaker is open")]
    CircuitBreakerOpen,
    
    #[error("Operation timeout after {0:?}")]
    Timeout(Duration),
    
    #[error("Operation error: {0}")]
    OperationError(String),
}

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: usize,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, requests flow normally
    Closed,
    /// Circuit is open, requests are rejected
    Open,
    /// Circuit is half-open, testing if service recovered
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Failure threshold to open circuit
    pub failure_threshold: u64,
    /// Success threshold to close circuit
    pub success_threshold: u64,
    /// Timeout before attempting half-open
    pub timeout: Duration,
    /// Window size for failure counting
    pub window_size: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            timeout: Duration::from_secs(60),
            window_size: 10,
        }
    }
}

/// Circuit breaker for failing operations
pub struct CircuitBreaker {
    /// Current circuit state
    state: RwLock<CircuitState>,
    /// Failure count
    failure_count: AtomicU64,
    /// Success count (in half-open state)
    success_count: AtomicU64,
    /// Last state change time
    last_state_change: RwLock<Instant>,
    /// Configuration
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_state_change: RwLock::new(Instant::now()),
            config,
        }
    }
    
    /// Check if operation can proceed
    pub fn can_proceed(&self) -> Result<(), ResilienceError> {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => Ok(()),
            CircuitState::HalfOpen => Ok(()),
            CircuitState::Open => {
                // Check if timeout has elapsed
                let elapsed = self.last_state_change.read().elapsed();
                if elapsed >= self.config.timeout {
                    self.transition_to_half_open();
                    Ok(())
                } else {
                    Err(ResilienceError::CircuitBreakerOpen)
                }
            }
        }
    }
    
    /// Record operation success
    pub fn record_success(&self) {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.config.success_threshold {
                    self.transition_to_closed();
                }
            }
            CircuitState::Open => {
                // Ignore successes in open state
            }
        }
    }
    
    /// Record operation failure
    pub fn record_failure(&self) {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.config.failure_threshold {
                    self.transition_to_open();
                }
            }
            CircuitState::HalfOpen => {
                // Single failure in half-open means service still unhealthy
                self.transition_to_open();
            }
            CircuitState::Open => {
                // Already open, nothing to do
            }
        }
    }
    
    /// Get current circuit state
    pub fn state(&self) -> CircuitState {
        *self.state.read()
    }
    
    fn transition_to_open(&self) {
        let mut state = self.state.write();
        *state = CircuitState::Open;
        *self.last_state_change.write() = Instant::now();
        self.success_count.store(0, Ordering::SeqCst);
        tracing::warn!("Circuit breaker transitioned to OPEN");
    }
    
    fn transition_to_half_open(&self) {
        let mut state = self.state.write();
        *state = CircuitState::HalfOpen;
        *self.last_state_change.write() = Instant::now();
        self.success_count.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        tracing::info!("Circuit breaker transitioned to HALF_OPEN");
    }
    
    fn transition_to_closed(&self) {
        let mut state = self.state.write();
        *state = CircuitState::Closed;
        *self.last_state_change.write() = Instant::now();
        self.success_count.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        tracing::info!("Circuit breaker transitioned to CLOSED");
    }
}

/// Retry with exponential backoff
pub async fn retry_with_backoff<F, T, E>(
    config: RetryConfig,
    mut operation: F,
) -> Result<T, ResilienceError>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    let mut backoff = config.initial_backoff;
    
    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) if attempt >= config.max_retries => {
                tracing::error!("Operation failed after {} retries: {}", attempt, e);
                return Err(ResilienceError::MaxRetriesExceeded(attempt));
            }
            Err(e) => {
                tracing::warn!(
                    "Operation failed (attempt {}/{}): {}. Retrying in {:?}",
                    attempt + 1,
                    config.max_retries,
                    e,
                    backoff
                );
                
                tokio::time::sleep(backoff).await;
                
                // Exponential backoff with cap
                backoff = std::cmp::min(
                    Duration::from_secs_f64(backoff.as_secs_f64() * config.backoff_multiplier),
                    config.max_backoff,
                );
                
                attempt += 1;
            }
        }
    }
}

/// Execute operation with circuit breaker
pub async fn with_circuit_breaker<F, T, E>(
    circuit_breaker: &CircuitBreaker,
    operation: F,
) -> Result<T, ResilienceError>
where
    F: FnOnce() -> Result<T, E>,
    E: std::fmt::Display,
{
    // Check if circuit allows operation
    circuit_breaker.can_proceed()?;
    
    // Execute operation
    match operation() {
        Ok(result) => {
            circuit_breaker.record_success();
            Ok(result)
        }
        Err(e) => {
            circuit_breaker.record_failure();
            Err(ResilienceError::OperationError(e.to_string()))
        }
    }
}

/// Timeout wrapper for operations
pub async fn with_timeout<F, T>(
    duration: Duration,
    operation: F,
) -> Result<T, ResilienceError>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(duration, operation)
        .await
        .map_err(|_| ResilienceError::Timeout(duration))
}

/// Graceful degradation with fallback
pub async fn with_fallback<F, T, E, FB>(
    primary: F,
    fallback: FB,
) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E>,
    FB: FnOnce() -> Result<T, E>,
{
    match primary() {
        Ok(result) => Ok(result),
        Err(_) => {
            tracing::warn!("Primary operation failed, using fallback");
            fallback()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    
    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let breaker = CircuitBreaker::new(config);
        
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.can_proceed().is_ok());
        
        // Record failures
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);
        
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);
        
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        
        // Should reject requests
        assert!(breaker.can_proceed().is_err());
    }
    
    #[test]
    fn test_circuit_breaker_half_open_to_closed() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let breaker = CircuitBreaker::new(config);
        
        // Open the circuit
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        
        // Wait for timeout
        std::thread::sleep(Duration::from_millis(20));
        
        // Should allow half-open
        assert!(breaker.can_proceed().is_ok());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
        
        // Record successes
        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
        
        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }
    
    #[tokio::test]
    async fn test_retry_with_backoff_success() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            backoff_multiplier: 2.0,
        };
        
        let result = retry_with_backoff(config, || {
            let count = counter_clone.fetch_add(1, AtomicOrdering::SeqCst);
            if count < 2 {
                Err("Not yet")
            } else {
                Ok(42)
            }
        })
        .await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(AtomicOrdering::SeqCst), 3);
    }
    
    #[tokio::test]
    async fn test_retry_with_backoff_failure() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        
        let config = RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            backoff_multiplier: 2.0,
        };
        
        let result = retry_with_backoff(config, || -> Result<i32, &'static str> {
            counter_clone.fetch_add(1, AtomicOrdering::SeqCst);
            Err("Always fails")
        })
        .await;
        
        assert!(result.is_err());
        // Should have tried max_retries + 1 times (initial + retries)
        assert_eq!(counter.load(AtomicOrdering::SeqCst), 3);
    }
    
    #[tokio::test]
    async fn test_with_timeout_success() {
        let result = with_timeout(Duration::from_secs(1), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            42
        })
        .await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
    
    #[tokio::test]
    async fn test_with_timeout_failure() {
        let result = with_timeout(Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            42
        })
        .await;
        
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_with_fallback() {
        // Primary succeeds
        let result = with_fallback(|| Ok::<_, String>(42), || Ok(0)).await;
        assert_eq!(result.unwrap(), 42);
        
        // Primary fails, fallback succeeds
        let result = with_fallback(
            || Err::<i32, _>("Primary failed"),
            || Ok(99),
        )
        .await;
        assert_eq!(result.unwrap(), 99);
    }
}

