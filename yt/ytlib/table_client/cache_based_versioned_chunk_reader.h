#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/client/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/core/misc/range.h>
#include <yt/core/misc/linear_probe.h>

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
    const TChunkStatePtr& state,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TSharedRange<TKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

IVersionedReaderPtr CreateCacheBasedVersionedChunkReader(
    const TChunkStatePtr& state,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
