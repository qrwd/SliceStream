//! FNN JSON-RPC client placeholder crate.

pub mod client;

pub use client::{
    FiberInvoice, FiberPayment, FiberRpcClient, FiberRpcError, FiberRpcPing, FiberSettlementRecord,
    JsonRpcRequest, RpcErrorCode,
};
