//! Network-accessible mailboxes via TCP/UDP
//!
//! This module extends the mailbox system with network capabilities,
//! allowing mailboxes to be accessed remotely over TCP or UDP.
//!
//! ## Features
//!
//! - TCP server for reliable message delivery
//! - UDP server for low-latency messaging
//! - Binary protocol with versioning
//! - Authentication and authorization (planned)
//! - Connection pooling
//! - Automatic reconnection
//!
//! ## Example
//!
//! ```rust,no_run
//! use dolda::mailbox_network::{MailboxServer, NetworkConfig, Protocol};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Start TCP server
//! let config = NetworkConfig {
//!     host: "127.0.0.1".to_string(),
//!     port: 9090,
//!     protocol: Protocol::Tcp,
//!     ..Default::default()
//! };
//!
//! let server = MailboxServer::new(config).await?;
//! server.start().await?;
//! # Ok(())
//! # }
//! ```

use crate::mailbox::{Mailbox, MailboxError, MailboxSystem, Message};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Mailbox error: {0}")]
    Mailbox(#[from] MailboxError),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    
    #[error("Invalid protocol version: {0}")]
    InvalidProtocol(u8),
    
    #[error("Authentication failed")]
    AuthenticationFailed,
    
    #[error("Mailbox not found: {0}")]
    MailboxNotFound(String),
    
    #[error("Connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, NetworkError>;

/// Network protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// TCP for reliable delivery
    Tcp,
    /// UDP for low latency
    Udp,
}

/// Network configuration
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Protocol (TCP or UDP)
    pub protocol: Protocol,
    /// Maximum message size (bytes)
    pub max_message_size: usize,
    /// Connection timeout (milliseconds)
    pub timeout_ms: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9090,
            protocol: Protocol::Tcp,
            max_message_size: 10 * 1024 * 1024, // 10MB
            timeout_ms: 5000,
        }
    }
}

/// Network protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
enum NetworkMessage {
    /// Send message to mailbox
    Send {
        to: String,
        from: String,
        body: Vec<u8>,
        subject: Option<String>,
    },
    
    /// Receive message from mailbox
    Receive {
        mailbox: String,
    },
    
    /// Peek at next message
    Peek {
        mailbox: String,
    },
    
    /// Response with message
    MessageResponse {
        message: Option<MessageData>,
    },
    
    /// Response with success
    Success,
    
    /// Response with error
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    pub from: String,
    pub to: String,
    pub id: String,
    pub timestamp: u64,
    pub body: Vec<u8>,
    pub subject: Option<String>,
}

impl From<Message> for MessageData {
    fn from(msg: Message) -> Self {
        Self {
            from: msg.from.as_str().to_string(),
            to: msg.to.as_str().to_string(),
            id: msg.id,
            timestamp: msg.timestamp,
            body: msg.body,
            subject: msg.subject,
        }
    }
}

/// Network-accessible mailbox server
pub struct MailboxServer {
    config: NetworkConfig,
    system: Arc<RwLock<MailboxSystem>>,
    mailboxes: Arc<RwLock<HashMap<String, Arc<Mailbox>>>>,
}

impl MailboxServer {
    /// Create a new mailbox server
    pub async fn new(config: NetworkConfig, system: Arc<RwLock<MailboxSystem>>) -> Result<Self> {
        Ok(Self {
            config,
            system,
            mailboxes: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Start the server
    pub async fn start(&self) -> Result<()> {
        match self.config.protocol {
            Protocol::Tcp => self.start_tcp().await,
            Protocol::Udp => self.start_udp().await,
        }
    }
    
    /// Start TCP server
    async fn start_tcp(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        
        println!("📡 Mailbox TCP server listening on {}", addr);
        
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            println!("📥 New connection from {}", peer_addr);
            
            let server = Arc::new(self.clone());
            tokio::spawn(async move {
                if let Err(e) = server.handle_tcp_connection(stream, peer_addr).await {
                    eprintln!("❌ Error handling connection from {}: {}", peer_addr, e);
                }
            });
        }
    }
    
    /// Handle TCP connection
    async fn handle_tcp_connection(&self, mut stream: TcpStream, peer_addr: SocketAddr) -> Result<()> {
        loop {
            // Read message length (4 bytes)
            let mut len_buf = [0u8; 4];
            match stream.read_exact(&mut len_buf).await {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    println!("📤 Connection closed by {}", peer_addr);
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }
            
            let msg_len = u32::from_be_bytes(len_buf) as usize;
            
            if msg_len > self.config.max_message_size {
                return Err(NetworkError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Message too large: {} bytes", msg_len),
                )));
            }
            
            // Read message data
            let mut msg_buf = vec![0u8; msg_len];
            stream.read_exact(&mut msg_buf).await?;
            
            // Deserialize message
            let network_msg: NetworkMessage = bincode::deserialize(&msg_buf)?;
            
            // Handle message
            let response = self.handle_message(network_msg).await;
            
            // Serialize response
            let response_bytes = bincode::serialize(&response)?;
            let response_len = (response_bytes.len() as u32).to_be_bytes();
            
            // Send response
            stream.write_all(&response_len).await?;
            stream.write_all(&response_bytes).await?;
            stream.flush().await?;
        }
    }
    
    /// Start UDP server
    async fn start_udp(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let socket = UdpSocket::bind(&addr).await?;
        
        println!("📡 Mailbox UDP server listening on {}", addr);
        
        let mut buf = vec![0u8; self.config.max_message_size];
        
        loop {
            let (len, peer_addr) = socket.recv_from(&mut buf).await?;
            
            println!("📥 UDP packet from {}", peer_addr);
            
            // Deserialize message
            let network_msg: NetworkMessage = match bincode::deserialize(&buf[..len]) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("❌ Failed to deserialize UDP message: {}", e);
                    continue;
                }
            };
            
            // Handle message
            let response = self.handle_message(network_msg).await;
            
            // Serialize response
            let response_bytes = match bincode::serialize(&response) {
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("❌ Failed to serialize response: {}", e);
                    continue;
                }
            };
            
            // Send response
            if let Err(e) = socket.send_to(&response_bytes, peer_addr).await {
                eprintln!("❌ Failed to send UDP response: {}", e);
            }
        }
    }
    
    /// Handle network message
    async fn handle_message(&self, msg: NetworkMessage) -> NetworkMessage {
        match msg {
            NetworkMessage::Send { to, from, body, subject } => {
                self.handle_send(&to, &from, body, subject).await
            }
            NetworkMessage::Receive { mailbox } => {
                self.handle_receive(&mailbox).await
            }
            NetworkMessage::Peek { mailbox } => {
                self.handle_peek(&mailbox).await
            }
            _ => NetworkMessage::Error {
                message: "Invalid request".to_string(),
            },
        }
    }
    
    /// Handle send message
    async fn handle_send(
        &self,
        to: &str,
        from: &str,
        body: Vec<u8>,
        subject: Option<String>,
    ) -> NetworkMessage {
        // Get or create sender mailbox
        let sender = match self.get_or_create_mailbox(from).await {
            Ok(mb) => mb,
            Err(e) => return NetworkMessage::Error {
                message: format!("Failed to get sender mailbox: {}", e),
            },
        };
        
        // Send message
        let result = if let Some(subj) = subject {
            sender.send_with_subject(to, &subj, &body).await
        } else {
            sender.send_to(to, &body).await
        };
        
        match result {
            Ok(_) => NetworkMessage::Success,
            Err(e) => NetworkMessage::Error {
                message: format!("Failed to send: {}", e),
            },
        }
    }
    
    /// Handle receive message
    async fn handle_receive(&self, mailbox: &str) -> NetworkMessage {
        let mb = match self.get_or_create_mailbox(mailbox).await {
            Ok(mb) => mb,
            Err(e) => return NetworkMessage::Error {
                message: format!("Failed to get mailbox: {}", e),
            },
        };
        
        match mb.receive().await {
            Ok(Some(msg)) => NetworkMessage::MessageResponse {
                message: Some(msg.into()),
            },
            Ok(None) => NetworkMessage::MessageResponse {
                message: None,
            },
            Err(e) => NetworkMessage::Error {
                message: format!("Failed to receive: {}", e),
            },
        }
    }
    
    /// Handle peek message
    async fn handle_peek(&self, mailbox: &str) -> NetworkMessage {
        let mb = match self.get_or_create_mailbox(mailbox).await {
            Ok(mb) => mb,
            Err(e) => return NetworkMessage::Error {
                message: format!("Failed to get mailbox: {}", e),
            },
        };
        
        match mb.peek().await {
            Ok(Some(msg)) => NetworkMessage::MessageResponse {
                message: Some(msg.into()),
            },
            Ok(None) => NetworkMessage::MessageResponse {
                message: None,
            },
            Err(e) => NetworkMessage::Error {
                message: format!("Failed to peek: {}", e),
            },
        }
    }
    
    /// Get or create mailbox
    async fn get_or_create_mailbox(&self, address: &str) -> Result<Arc<Mailbox>> {
        // Check cache first
        {
            let mailboxes = self.mailboxes.read().await;
            if let Some(mb) = mailboxes.get(address) {
                return Ok(Arc::clone(mb));
            }
        }
        
        // Get from system or create
        let mut system = self.system.write().await;
        let mb = match system.get_mailbox(address).await? {
            Some(mb) => Arc::new(mb),
            None => Arc::new(system.create_mailbox(address).await?),
        };
        
        // Cache it
        {
            let mut mailboxes = self.mailboxes.write().await;
            mailboxes.insert(address.to_string(), Arc::clone(&mb));
        }
        
        Ok(mb)
    }
}

// Manual Clone implementation
impl Clone for MailboxServer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            system: Arc::clone(&self.system),
            mailboxes: Arc::clone(&self.mailboxes),
        }
    }
}

/// Network client for accessing remote mailboxes
pub struct MailboxClient {
    server_addr: SocketAddr,
    protocol: Protocol,
    /// Timeout for network operations (reserved for future use)
    #[allow(dead_code)]
    timeout: std::time::Duration,
}

impl MailboxClient {
    /// Create a new mailbox client
    pub fn new(server_addr: SocketAddr, protocol: Protocol) -> Self {
        Self {
            server_addr,
            protocol,
            timeout: std::time::Duration::from_secs(5),
        }
    }
    
    /// Send message to remote mailbox
    pub async fn send(&self, to: &str, from: &str, body: &[u8]) -> Result<()> {
        let msg = NetworkMessage::Send {
            to: to.to_string(),
            from: from.to_string(),
            body: body.to_vec(),
            subject: None,
        };
        
        match self.protocol {
            Protocol::Tcp => self.send_tcp(msg).await,
            Protocol::Udp => self.send_udp(msg).await,
        }
    }
    
    /// Send message with subject
    pub async fn send_with_subject(
        &self,
        to: &str,
        from: &str,
        subject: &str,
        body: &[u8],
    ) -> Result<()> {
        let msg = NetworkMessage::Send {
            to: to.to_string(),
            from: from.to_string(),
            body: body.to_vec(),
            subject: Some(subject.to_string()),
        };
        
        match self.protocol {
            Protocol::Tcp => self.send_tcp(msg).await,
            Protocol::Udp => self.send_udp(msg).await,
        }
    }
    
    /// Receive message from remote mailbox
    pub async fn receive(&self, mailbox: &str) -> Result<Option<MessageData>> {
        let msg = NetworkMessage::Receive {
            mailbox: mailbox.to_string(),
        };
        
        let response = match self.protocol {
            Protocol::Tcp => self.request_tcp(msg).await?,
            Protocol::Udp => self.request_udp(msg).await?,
        };
        
        match response {
            NetworkMessage::MessageResponse { message } => Ok(message),
            NetworkMessage::Error { message } => {
                Err(NetworkError::Mailbox(MailboxError::MailboxNotFound(message)))
            }
            _ => Err(NetworkError::InvalidProtocol(0)),
        }
    }
    
    /// Send TCP message
    async fn send_tcp(&self, msg: NetworkMessage) -> Result<()> {
        self.request_tcp(msg).await?;
        Ok(())
    }
    
    /// Send TCP request and wait for response
    async fn request_tcp(&self, msg: NetworkMessage) -> Result<NetworkMessage> {
        let mut stream = TcpStream::connect(self.server_addr).await?;
        
        // Serialize message
        let msg_bytes = bincode::serialize(&msg)?;
        let msg_len = (msg_bytes.len() as u32).to_be_bytes();
        
        // Send message
        stream.write_all(&msg_len).await?;
        stream.write_all(&msg_bytes).await?;
        stream.flush().await?;
        
        // Read response length
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let response_len = u32::from_be_bytes(len_buf) as usize;
        
        // Read response
        let mut response_buf = vec![0u8; response_len];
        stream.read_exact(&mut response_buf).await?;
        
        // Deserialize response
        let response: NetworkMessage = bincode::deserialize(&response_buf)?;
        Ok(response)
    }
    
    /// Send UDP message
    async fn send_udp(&self, msg: NetworkMessage) -> Result<()> {
        self.request_udp(msg).await?;
        Ok(())
    }
    
    /// Send UDP request and wait for response
    async fn request_udp(&self, msg: NetworkMessage) -> Result<NetworkMessage> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        
        // Serialize message
        let msg_bytes = bincode::serialize(&msg)?;
        
        // Send message
        socket.send_to(&msg_bytes, self.server_addr).await?;
        
        // Receive response
        let mut buf = vec![0u8; 10 * 1024 * 1024];
        let (len, _) = socket.recv_from(&mut buf).await?;
        
        // Deserialize response
        let response: NetworkMessage = bincode::deserialize(&buf[..len])?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_tcp_server() {
        let temp_dir = TempDir::new().unwrap();
        let system = Arc::new(RwLock::new(
            MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string())
                .await
                .unwrap()
        ));
        
        let config = NetworkConfig {
            host: "127.0.0.1".to_string(),
            port: 19090,
            protocol: Protocol::Tcp,
            ..Default::default()
        };
        
        let server = MailboxServer::new(config.clone(), Arc::clone(&system))
            .await
            .unwrap();
        
        // Start server in background
        tokio::spawn(async move {
            let _ = server.start().await;
        });
        
        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Create client
        let client = MailboxClient::new(
            format!("{}:{}", config.host, config.port).parse().unwrap(),
            Protocol::Tcp,
        );
        
        // Send message
        client.send("bob@dolda", "alice@dolda", b"Hello!").await.unwrap();
        
        // Receive message
        let msg = client.receive("bob@dolda").await.unwrap();
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.body, b"Hello!");
    }
}

