//! Inter-thread messaging primitives.

pub mod ring_buffer;

pub use ring_buffer::{Consumer, Producer, RingBuffer};
