//! Cacheline-aligned memory arena.

pub mod slab;

pub use slab::{SlabBlock, SlabPool, CACHELINE, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE, NUM_CLASSES};
