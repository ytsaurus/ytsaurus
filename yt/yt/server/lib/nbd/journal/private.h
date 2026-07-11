#pragma once

#include "public.h"

#include <library/cpp/yt/misc/strong_typedef.h>

#include <util/system/types.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! An opaque block id stored in IBlockStore.
YT_DEFINE_STRONG_TYPEDEF(TStoredBlockId, ui64);

//! Bit layout of the 64-bit stored block id, from most to least significant:
//!   [ 4 bits] reserved (always zero)
//!   [20 bits] chunk index
//!   [30 bits] record index within the chunk
//!   [10 bits] fragment (block) index within the record
namespace NStoredBlockIdLayout {

constexpr int ReservedBits = 4;
constexpr int ChunkIndexBits = 20;
constexpr int RecordIndexBits = 30;
constexpr int FragmentIndexBits = 10;
static_assert(ReservedBits + ChunkIndexBits + RecordIndexBits + FragmentIndexBits == 64);

} // namespace NStoredBlockIdLayout

// Capacity limits implied by the id layout above.

//! Total number of chunks a store may ever allocate; exceeding it is fatal.
constexpr i64 MaxChunksPerDevice = 1LL << NStoredBlockIdLayout::ChunkIndexBits;

//! Number of records that fit into a single chunk before it must be retired.
constexpr i64 MaxRecordsPerChunk = 1LL << NStoredBlockIdLayout::RecordIndexBits;

//! Number of blocks (fragments) that can be coalesced into a single record.
constexpr i64 MaxFragmentsPerRecord = 1LL << NStoredBlockIdLayout::FragmentIndexBits;

DECLARE_REFCOUNTED_STRUCT(IBlockStore)

//! An opaque block id stored in IDirtyBlockPool.
YT_DEFINE_STRONG_TYPEDEF(TDirtyBlockId, ui64);

struct TDirtyBlock;
using TDirtyBlockPtr = TIntrusivePtr<TDirtyBlock>;

DECLARE_REFCOUNTED_STRUCT(IDirtyBlockPool)
DECLARE_REFCOUNTED_STRUCT(IBlockMap)
DECLARE_REFCOUNTED_STRUCT(IBlockFlusher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
