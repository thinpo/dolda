//! Network layer for distributed DOLDA communication
//!
//! This module provides:
//! - Binary protocol with versioning
//! - Node-to-node RPC communication
//! - Connection pooling
//! - Message framing and serialization

use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use thiserror::Error;
use dashmap::DashMap;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Protocol error: unsupported version {0}")]
    UnsupportedVersion(u16),
    
    #[error("Invalid message: {0}")]
    InvalidMessage(String),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Timeout")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, NetworkError>;

/// Protocol version
pub const PROTOCOL_VERSION: u16 = 1;

/// Magic bytes for protocol identification
pub const PROTOCOL_MAGIC: u32 = 0xD01DA001; // DOLDA 001

/// Message types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum MessageType {
    /// Ping request
    Ping = 1,
    /// Pong response
    Pong = 2,
    /// Append record request
    AppendRequest = 10,
    /// Append record response
    AppendResponse = 11,
    /// Read record request
    ReadRequest = 12,
    /// Read record response
    ReadResponse = 13,
    /// Replication request
    ReplicateRequest = 20,
    /// Replication response
    ReplicateResponse = 21,
    /// Health check request
    HealthRequest = 30,
    /// Health check response
    HealthResponse = 31,
    /// Error response
    Error = 255,
}

/// Message header structure
#[derive(Debug, Clone)]
pub struct MessageHeader {
    /// Magic bytes
    pub magic: u32,
    /// Protocol version
    pub version: u16,
    /// Message type
    pub msg_type: MessageType,
    /// Payload length
    pub payload_len: u32,
    /// Request ID
    pub request_id: u64,
}

impl MessageHeader {
    /// Header size in bytes
    pub const SIZE: usize = 4 + 2 + 2 + 4 + 8; // 20 bytes
    
    /// Create a new message header
    pub fn new(msg_type: MessageType, payload_len: u32, request_id: u64) -> Self {
        Self {
            magic: PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            msg_type,
            payload_len,
            request_id,
        }
    }
    
    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.magic.to_be_bytes());
        bytes[4..6].copy_from_slice(&self.version.to_be_bytes());
        bytes[6..8].copy_from_slice(&(self.msg_type as u16).to_be_bytes());
        bytes[8..12].copy_from_slice(&self.payload_len.to_be_bytes());
        bytes[12..20].copy_from_slice(&self.request_id.to_be_bytes());
        bytes
    }
    
    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(NetworkError::InvalidMessage(
                format!("Header too short: {} bytes", bytes.len())
            ));
        }
        
        let magic = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        if magic != PROTOCOL_MAGIC {
            return Err(NetworkError::InvalidMessage(
                format!("Invalid magic: {:#x}", magic)
            ));
        }
        
        let version = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
        if version != PROTOCOL_VERSION {
            return Err(NetworkError::UnsupportedVersion(version));
        }
        
        let msg_type_raw = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
        let msg_type = match msg_type_raw {
            1 => MessageType::Ping,
            2 => MessageType::Pong,
            10 => MessageType::AppendRequest,
            11 => MessageType::AppendResponse,
            12 => MessageType::ReadRequest,
            13 => MessageType::ReadResponse,
            20 => MessageType::ReplicateRequest,
            21 => MessageType::ReplicateResponse,
            30 => MessageType::HealthRequest,
            31 => MessageType::HealthResponse,
            255 => MessageType::Error,
            _ => return Err(NetworkError::InvalidMessage(
                format!("Unknown message type: {}", msg_type_raw)
            )),
        };
        
        let payload_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let request_id = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
        
        Ok(Self {
            magic,
            version,
            msg_type,
            payload_len,
            request_id,
        })
    }
}

/// Network message
#[derive(Debug, Clone)]
pub struct Message {
    /// Message header
    pub header: MessageHeader,
    /// Payload bytes
    pub payload: Vec<u8>,
}

impl Message {
    /// Create a new message
    pub fn new(msg_type: MessageType, payload: Vec<u8>, request_id: u64) -> Self {
        let header = MessageHeader::new(msg_type, payload.len() as u32, request_id);
        Self { header, payload }
    }
    
    /// Serialize message to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MessageHeader::SIZE + self.payload.len());
        bytes.extend_from_slice(&self.header.to_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }
    
    /// Read message from stream
    pub async fn read_from(stream: &mut TcpStream) -> Result<Self> {
        // Read header
        let mut header_bytes = [0u8; MessageHeader::SIZE];
        stream.read_exact(&mut header_bytes).await?;
        let header = MessageHeader::from_bytes(&header_bytes)?;
        
        // Read payload
        let mut payload = vec![0u8; header.payload_len as usize];
        stream.read_exact(&mut payload).await?;
        
        Ok(Self { header, payload })
    }
    
    /// Write message to stream
    pub async fn write_to(&self, stream: &mut TcpStream) -> Result<()> {
        let bytes = self.to_bytes();
        stream.write_all(&bytes).await?;
        stream.flush().await?;
        Ok(())
    }
}

/// Connection to a remote node
pub struct Connection {
    /// Remote address
    addr: SocketAddr,
    /// TCP stream
    stream: TcpStream,
    /// Request ID counter
    next_request_id: Arc<std::sync::atomic::AtomicU64>,
}

impl Connection {
    /// Connect to a remote node
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            addr,
            stream,
            next_request_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        })
    }
    
    /// Send a message and receive response
    pub async fn send_request(&mut self, msg_type: MessageType, payload: Vec<u8>) -> Result<Message> {
        let request_id = self.next_request_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let request = Message::new(msg_type, payload, request_id);
        
        // Send request
        request.write_to(&mut self.stream).await?;
        
        // Read response
        let response = Message::read_from(&mut self.stream).await?;
        
        // Verify request ID matches
        if response.header.request_id != request_id {
            return Err(NetworkError::InvalidMessage(
                format!("Request ID mismatch: expected {}, got {}", request_id, response.header.request_id)
            ));
        }
        
        Ok(response)
    }
    
    /// Send a ping
    pub async fn ping(&mut self) -> Result<()> {
        let response = self.send_request(MessageType::Ping, vec![]).await?;
        
        if response.header.msg_type != MessageType::Pong {
            return Err(NetworkError::InvalidMessage(
                format!("Expected Pong, got {:?}", response.header.msg_type)
            ));
        }
        
        Ok(())
    }
}

/// Connection pool for managing connections to multiple nodes
pub struct ConnectionPool {
    /// Active connections
    connections: DashMap<SocketAddr, Arc<tokio::sync::Mutex<Connection>>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
        }
    }
    
    /// Get or create a connection to a node
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<Arc<tokio::sync::Mutex<Connection>>> {
        if let Some(conn) = self.connections.get(&addr) {
            return Ok(Arc::clone(&conn));
        }
        
        // Create new connection
        let conn = Connection::connect(addr).await?;
        let conn_arc = Arc::new(tokio::sync::Mutex::new(conn));
        
        self.connections.insert(addr, Arc::clone(&conn_arc));
        
        Ok(conn_arc)
    }
    
    /// Remove a connection from the pool
    pub fn remove_connection(&self, addr: &SocketAddr) {
        self.connections.remove(addr);
    }
    
    /// Get number of active connections
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
}

/// Network server for handling incoming connections
pub struct NetworkServer {
    /// Listen address
    addr: SocketAddr,
    /// Message handler
    handler: Arc<dyn MessageHandler + Send + Sync>,
}

/// Message handler trait
#[async_trait::async_trait]
pub trait MessageHandler {
    /// Handle incoming message
    async fn handle_message(&self, msg_type: MessageType, payload: Vec<u8>) -> Result<Vec<u8>>;
}

impl NetworkServer {
    /// Create a new network server
    pub fn new(addr: SocketAddr, handler: Arc<dyn MessageHandler + Send + Sync>) -> Self {
        Self { addr, handler }
    }
    
    /// Start the server
    pub async fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Network server listening on {}", self.addr);
        
        loop {
            let (stream, remote_addr) = listener.accept().await?;
            tracing::debug!("Accepted connection from {}", remote_addr);
            
            let handler = Arc::clone(&self.handler);
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(stream, handler).await {
                    tracing::error!("Connection error from {}: {}", remote_addr, e);
                }
            });
        }
    }
    
    async fn handle_connection(
        mut stream: TcpStream,
        handler: Arc<dyn MessageHandler + Send + Sync>,
    ) -> Result<()> {
        loop {
            // Read message
            let message = match Message::read_from(&mut stream).await {
                Ok(msg) => msg,
                Err(NetworkError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Connection closed
                    break;
                }
                Err(e) => return Err(e),
            };
            
            // Handle message
            let response_payload = handler
                .handle_message(message.header.msg_type, message.payload)
                .await?;
            
            // Send response
            let response = Message::new(
                Self::get_response_type(message.header.msg_type),
                response_payload,
                message.header.request_id,
            );
            
            response.write_to(&mut stream).await?;
        }
        
        Ok(())
    }
    
    fn get_response_type(request_type: MessageType) -> MessageType {
        match request_type {
            MessageType::Ping => MessageType::Pong,
            MessageType::AppendRequest => MessageType::AppendResponse,
            MessageType::ReadRequest => MessageType::ReadResponse,
            MessageType::ReplicateRequest => MessageType::ReplicateResponse,
            MessageType::HealthRequest => MessageType::HealthResponse,
            _ => MessageType::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_message_header_roundtrip() {
        let header = MessageHeader::new(MessageType::Ping, 100, 42);
        let bytes = header.to_bytes();
        let decoded = MessageHeader::from_bytes(&bytes).unwrap();
        
        assert_eq!(decoded.magic, PROTOCOL_MAGIC);
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.msg_type, MessageType::Ping);
        assert_eq!(decoded.payload_len, 100);
        assert_eq!(decoded.request_id, 42);
    }
    
    #[test]
    fn test_message_serialization() {
        let payload = vec![1, 2, 3, 4, 5];
        let message = Message::new(MessageType::AppendRequest, payload.clone(), 123);
        let bytes = message.to_bytes();
        
        assert_eq!(bytes.len(), MessageHeader::SIZE + payload.len());
        assert_eq!(&bytes[0..4], &PROTOCOL_MAGIC.to_be_bytes());
    }
    
    #[test]
    fn test_invalid_magic() {
        let mut bytes = [0u8; MessageHeader::SIZE];
        bytes[0..4].copy_from_slice(&0xDEADBEEFu32.to_be_bytes());
        
        let result = MessageHeader::from_bytes(&bytes);
        assert!(matches!(result, Err(NetworkError::InvalidMessage(_))));
    }
    
    #[test]
    fn test_unsupported_version() {
        let mut bytes = [0u8; MessageHeader::SIZE];
        bytes[0..4].copy_from_slice(&PROTOCOL_MAGIC.to_be_bytes());
        bytes[4..6].copy_from_slice(&999u16.to_be_bytes());
        
        let result = MessageHeader::from_bytes(&bytes);
        assert!(matches!(result, Err(NetworkError::UnsupportedVersion(999))));
    }
    
    #[tokio::test]
    async fn test_connection_pool() {
        let pool = ConnectionPool::new();
        assert_eq!(pool.connection_count(), 0);
        
        // Note: We can't actually connect in tests without a running server
        // This just tests the pool structure
    }
}

