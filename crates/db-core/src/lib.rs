#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod btree;
mod codec;
mod codec_helpers;
mod codec_primitives;
#[cfg(feature = "registry")]
mod codec_registry;
mod engine_types;
mod port;
mod range_merge;
mod simple_key;
#[cfg(feature = "test-helpers")]
mod test_helpers;
mod transaction_patch;

pub use btree::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};
pub use codec::BufferSink;
pub use codec::{
  CURRENT_CODEC_VERSION, DecodeError, FastKeyCodec, KeyCodec, KeyScratch, StorageCodec, ValueCodec,
  compare_encoded_keys, decode_value_to_vec, encode_key_into_scratch, encode_key_to_vec,
  encode_value_to_vec,
};

pub use codec_helpers::{decode_from_slice, decode_with_version, encode_with_version};
pub use codec_primitives::*;

pub use range_merge::merge_range_maps;

pub use engine_types::{EngineKey, EngineRow, EngineType, EngineValue};

pub use simple_key::{IntegerI64Codec, Tuple2Codec, Utf8Codec};

pub use transaction_patch::{
  TransactionEntry, TransactionPatch, commit_transaction_patch, merge_transaction_patch_range,
  patch_delete, patch_get, patch_insert, patch_remove,
};

#[cfg(feature = "registry")]
pub use codec_registry::{CodecRegistry, EncodedComparator, TypedComparatorAdapter};

#[cfg(feature = "test-helpers")]
pub use test_helpers::{MockBTree, block_on};

pub use port::{IntoStoragePort, StoragePort};
