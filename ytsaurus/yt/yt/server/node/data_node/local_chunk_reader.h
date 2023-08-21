#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader for a locally stored chunk.
/*!
 *  \note
 *  Thread affinity: any
 */
NChunkClient::IChunkReaderPtr CreateLocalChunkReader(
    NChunkClient::TReplicationReaderConfigPtr config,
    IChunkPtr chunk,
    NChunkClient::IBlockCachePtr blockCache,
    NTableClient::TBlockMetaCachePtr blockMetaCache);

////////////////////////////////////////////////////////////////////////////////

struct ILocalChunkFragmentReader
    : public NChunkClient::IChunkFragmentReader
{
    virtual TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILocalChunkFragmentReader)

ILocalChunkFragmentReaderPtr CreateLocalChunkFragmentReader(
    IChunkPtr chunk,
    bool useDirectIO);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
