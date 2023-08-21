#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/linear_probe.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Same as CreateVersionedChunkReader but only suitable for in-memory tables
//! since it relies on block cache to retrieve chunk blocks.

/*!
 *  For each block #blockCache must be able for provide either a compressed
 *  or uncompressed version.
 *
 *  The implementation is (kind of) highly optimized :)
 */

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    NChunkClient::TChunkId chunkId,
    const TChunkStatePtr& state,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TSharedRange<TLegacyKey> keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    NChunkClient::TChunkId chunkId,
    const TChunkStatePtr& state,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TSharedRange<TRowRange>& singletonClippingRange = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
