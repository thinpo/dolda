//! Health check and monitoring HTTP server
//!
//! Provides REST API endpoints for:
//! - Liveness checks (is the service running?)
//! - Readiness checks (can the service accept traffic?)
//! - Detailed health status
//! - System metrics

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

/// Health status of a component
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but functional
    Degraded,
    /// Component is unhealthy
    Unhealthy,
    /// Component status is unknown
    Unknown,
}

impl HealthStatus {
    /// Check if status is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }
    
    /// Convert to HTTP status code
    pub fn http_code(&self) -> u16 {
        match self {
            HealthStatus::Healthy => 200,
            HealthStatus::Degraded => 200, // Still accepting traffic
            HealthStatus::Unhealthy => 503,
            HealthStatus::Unknown => 503,
        }
    }
}

/// Component health details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Health status
    pub status: HealthStatus,
    /// Additional message
    pub message: String,
    /// Last check time (micros since epoch)
    pub last_check_us: u64,
}

/// Overall system health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    /// Overall status
    pub status: HealthStatus,
    /// Server uptime (seconds)
    pub uptime_seconds: u64,
    /// Individual component health
    pub components: Vec<ComponentHealth>,
    /// Version
    pub version: String,
}

/// Health check server state
pub struct HealthServer {
    /// Server start time
    start_time: Instant,
    /// Component health states
    components: Arc<RwLock<Vec<ComponentHealth>>>,
    /// Server version
    version: String,
}

impl HealthServer {
    /// Create a new health server
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            start_time: Instant::now(),
            components: Arc::new(RwLock::new(Vec::new())),
            version: version.into(),
        }
    }
    
    /// Register a component for health monitoring
    pub fn register_component(&self, name: impl Into<String>) {
        let component = ComponentHealth {
            name: name.into(),
            status: HealthStatus::Unknown,
            message: "Not yet checked".to_string(),
            last_check_us: 0,
        };
        self.components.write().push(component);
    }
    
    /// Update component health status
    pub fn update_component(&self, name: &str, status: HealthStatus, message: impl Into<String>) {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        
        let mut components = self.components.write();
        if let Some(comp) = components.iter_mut().find(|c| c.name == name) {
            comp.status = status;
            comp.message = message.into();
            comp.last_check_us = now_us;
        }
    }
    
    /// Get current system health
    pub fn get_health(&self) -> SystemHealth {
        let components = self.components.read().clone();
        
        // Overall status is worst component status (use max to get worst)
        let status = components.iter()
            .map(|c| c.status)
            .max_by_key(|s| match s {
                HealthStatus::Healthy => 0,
                HealthStatus::Degraded => 1,
                HealthStatus::Unhealthy => 2,
                HealthStatus::Unknown => 3,
            })
            .unwrap_or(HealthStatus::Unknown);
        
        SystemHealth {
            status,
            uptime_seconds: self.start_time.elapsed().as_secs(),
            components,
            version: self.version.clone(),
        }
    }
    
    /// Start health check HTTP server
    pub async fn serve(self: Arc<Self>, addr: &str) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Health check server listening on {}", addr);
        
        loop {
            let (mut socket, _) = listener.accept().await?;
            let server = Arc::clone(&self);
            
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];
                
                match socket.read(&mut buf).await {
                    Ok(n) if n > 0 => {
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let response = server.handle_request(&request);
                        let _ = socket.write_all(response.as_bytes()).await;
                    }
                    _ => {}
                }
            });
        }
    }
    
    /// Handle HTTP request (simple parser)
    fn handle_request(&self, request: &str) -> String {
        // Parse HTTP request line
        let first_line = request.lines().next().unwrap_or("");
        let parts: Vec<&str> = first_line.split_whitespace().collect();
        
        if parts.len() < 2 {
            return self.http_response(400, "Bad Request", "Invalid HTTP request");
        }
        
        let path = parts[1];
        
        match path {
            "/health" | "/health/live" => self.handle_liveness(),
            "/health/ready" => self.handle_readiness(),
            "/health/detail" => self.handle_detail(),
            "/" => self.http_response(200, "OK", "DOLDA Health Check Server"),
            _ => self.http_response(404, "Not Found", "Endpoint not found"),
        }
    }
    
    /// Handle liveness probe (is service running?)
    fn handle_liveness(&self) -> String {
        self.http_response(200, "OK", "alive")
    }
    
    /// Handle readiness probe (can service accept traffic?)
    fn handle_readiness(&self) -> String {
        let health = self.get_health();
        let code = health.status.http_code();
        let status = if code == 200 { "OK" } else { "Service Unavailable" };
        let body = if health.status.is_healthy() { "ready" } else { "not ready" };
        
        self.http_response(code, status, body)
    }
    
    /// Handle detailed health check
    fn handle_detail(&self) -> String {
        let health = self.get_health();
        let code = health.status.http_code();
        let status = if code == 200 { "OK" } else { "Service Unavailable" };
        
        // Serialize to JSON
        let json = serde_json::to_string_pretty(&health).unwrap_or_else(|_| "{}".to_string());
        
        self.http_json_response(code, status, &json)
    }
    
    /// Create HTTP response
    fn http_response(&self, code: u16, status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            code,
            status,
            body.len(),
            body
        )
    }
    
    /// Create HTTP JSON response
    fn http_json_response(&self, code: u16, status: &str, json: &str) -> String {
        format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            code,
            status,
            json.len(),
            json
        )
    }
}

/// Background health check runner
pub struct HealthChecker {
    server: Arc<HealthServer>,
    check_interval: Duration,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(server: Arc<HealthServer>, check_interval: Duration) -> Self {
        Self {
            server,
            check_interval,
        }
    }
    
    /// Start background health checks
    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.check_interval);
        
        loop {
            interval.tick().await;
            self.perform_checks().await;
        }
    }
    
    /// Perform all health checks
    async fn perform_checks(&self) {
        // Example checks - you would replace these with actual component checks
        
        // Check 1: Basic liveness
        self.server.update_component(
            "system",
            HealthStatus::Healthy,
            "System is running"
        );
        
        // Check 2: Memory usage (example)
        #[cfg(target_os = "linux")]
        {
            if let Ok(memory_status) = Self::check_memory() {
                self.server.update_component("memory", memory_status.0, memory_status.1);
            }
        }
        
        // Add more checks as needed
        tracing::debug!("Health checks completed");
    }
    
    /// Check memory usage
    #[cfg(target_os = "linux")]
    fn check_memory() -> Result<(HealthStatus, String), String> {
        use sysinfo::{System, RefreshKind, MemoryRefreshKind};
        
        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_memory(MemoryRefreshKind::everything())
        );
        sys.refresh_memory();
        
        let used_percent = (sys.used_memory() as f64 / sys.total_memory() as f64) * 100.0;
        
        let status = if used_percent < 80.0 {
            HealthStatus::Healthy
        } else if used_percent < 90.0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };
        
        Ok((status, format!("Memory usage: {:.1}%", used_percent)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_health_status() {
        assert!(HealthStatus::Healthy.is_healthy());
        assert!(!HealthStatus::Unhealthy.is_healthy());
        assert_eq!(HealthStatus::Healthy.http_code(), 200);
        assert_eq!(HealthStatus::Unhealthy.http_code(), 503);
    }
    
    #[test]
    fn test_component_registration() {
        let server = HealthServer::new("1.0.0");
        server.register_component("test");
        
        let health = server.get_health();
        assert_eq!(health.components.len(), 1);
        assert_eq!(health.components[0].name, "test");
    }
    
    #[test]
    fn test_component_update() {
        let server = HealthServer::new("1.0.0");
        server.register_component("test");
        server.update_component("test", HealthStatus::Healthy, "All good");
        
        let health = server.get_health();
        assert_eq!(health.components[0].status, HealthStatus::Healthy);
        assert_eq!(health.components[0].message, "All good");
    }
    
    #[test]
    fn test_system_health_aggregation() {
        let server = HealthServer::new("1.0.0");
        server.register_component("comp1");
        server.register_component("comp2");
        
        server.update_component("comp1", HealthStatus::Healthy, "Good");
        server.update_component("comp2", HealthStatus::Degraded, "Slow");
        
        let health = server.get_health();
        // Overall status should be degraded (worst of healthy and degraded)
        assert_eq!(health.status, HealthStatus::Degraded);
    }
    
    #[tokio::test]
    async fn test_http_parsing() {
        let server = HealthServer::new("1.0.0");
        
        let request = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let response = server.handle_request(request);
        assert!(response.contains("200 OK"));
        assert!(response.contains("alive"));
    }
}

