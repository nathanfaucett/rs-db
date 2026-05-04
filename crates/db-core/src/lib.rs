#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod blocking;
mod btree;
mod codec;
#[cfg(feature = "registry")]
mod codec_registry;
mod named_tree;
mod port;
mod range_merge;
mod simple_key;
#[cfg(feature = "test-helpers")]
mod test_helpers;
mod transaction_patch;

pub use blocking::block_on;
pub use btree::{BTree, BTreeError, BTreeExecutor, BTreeResult, BTreeTransaction};
pub use codec::{
  BufferSink, CURRENT_CODEC_VERSION, Cursor, DecodeError, FastKeyCodec, FastValueCodec, KeyCodec,
  KeyScratch, StorageCodec, ValueCodec, canonical_f64_bits, canonical_f64_bits_into_sink,
  compare_encoded_keys, decode_bool, decode_bytes, decode_from_slice, decode_len, decode_string,
  decode_usize, decode_value_to_vec, decode_version, decode_with_version, encode_bool,
  encode_bool_into_sink, encode_bytes, encode_bytes_into_sink, encode_i64, encode_i64_into_sink,
  encode_key_into_scratch, encode_key_to_vec, encode_len, encode_len_into_sink, encode_string,
  encode_string_into_sink, encode_u32, encode_u32_into_sink, encode_u64, encode_u64_into_sink,
  encode_usize, encode_usize_into_sink, encode_value_to_vec, encode_version,
  encode_version_into_sink, encode_with_version,
};

pub use range_merge::merge_range_maps;

pub use simple_key::{IntegerI64Codec, Tuple2Codec, Utf8Codec};

pub use transaction_patch::{TransactionEntry, TransactionPatch};

#[cfg(feature = "registry")]
pub use codec_registry::{CodecRegistry, EncodedComparator, TypedComparatorAdapter};

#[cfg(feature = "test-helpers")]
pub use test_helpers::MockBTree;

pub use port::{IntoStoragePort, StoragePort};

pub use named_tree::{NamedTreeProvider, NamedTreeTransaction};
