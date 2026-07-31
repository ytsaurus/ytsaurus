#pragma once

#include "public.h"

#include <library/cpp/yt/misc/strong_typedef.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/global.h>

#include <util/system/types.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Nbd");

////////////////////////////////////////////////////////////////////////////////

//! The block map's per-block value: an empty/dirty/stored tag with a TDirtyBlockId or TStoredBlockId
//! payload, packed into 64 bits (see NMappedBlockIdLayout).
YT_DEFINE_STRONG_TYPEDEF(TMappedBlockId, ui64);

//! Bit layout of the 64-bit mapped block id, from most to least significant:
//!   [ 1 bit ] CoW flag
//!   [ 2 bits] tag (empty, dirty, or stored)
//!   [61 bits] payload (a TDirtyBlockId or TStoredBlockId; zero when the tag is empty)
namespace NMappedBlockIdLayout {

constexpr int TagBits = 2;
constexpr int PayloadBits = 61;
static_assert(1 + TagBits + PayloadBits == 64);

constexpr ui64 PayloadMask = (1ULL << PayloadBits) - 1;
constexpr ui64 TagMask = ((1ULL << TagBits) - 1) << PayloadBits;
constexpr ui64 CoWMask = 1ULL << (PayloadBits + TagBits);

constexpr ui64 EmptyTag = 0;
constexpr ui64 DirtyTag = 1;
constexpr ui64 StoredTag = 2;

} // namespace NMappedBlockIdLayout

//! The mapped id of a block that has never been written.
constexpr auto EmptyMappedBlockId = TMappedBlockId(0);

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
constexpr int BlockIndexBits = 10;
static_assert(ReservedBits + ChunkIndexBits + RecordIndexBits + BlockIndexBits == 64);

} // namespace NStoredBlockIdLayout

// Capacity limits implied by the id layout above.

//! Total number of chunks a store may ever allocate; exceeding it is fatal.
constexpr i64 MaxChunksPerDevice = 1LL << NStoredBlockIdLayout::ChunkIndexBits;

//! Number of records that fit into a single chunk before it must be retired.
constexpr i64 MaxRecordsPerChunk = 1LL << NStoredBlockIdLayout::RecordIndexBits;

//! Number of blocks that can be coalesced into a single record.
constexpr i64 MaxBlocksPerRecord = 1LL << NStoredBlockIdLayout::BlockIndexBits;

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
