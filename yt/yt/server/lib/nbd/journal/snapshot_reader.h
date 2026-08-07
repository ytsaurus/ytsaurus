#pragma once

#include "private.h"
#include "block_store.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/memory/range.h>

#include <vector>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Reads the metadata of a device snapshot, streaming its block map batch by batch so a large device's
//! map is never materialized whole.
/*!
 *  Calls must not overlap: await each future before issuing the next.
 */
struct ISnapshotReader
    : public TRefCounted
{
    virtual TFuture<void> Open() = 0;

    //! Returns the next batch of the block map, ascending by block index (as stored in the block-index-sorted
    //! snapshot table). Empty once the table is drained.
    virtual TFuture<std::vector<TSnapshotBlock>> ReadBlocks() = 0;

    //! Returns the distinct journal chunks the blocks read so far reference.
    virtual std::vector<NChunkClient::TChunkId> GetReferencedChunkIds() const = 0;

    //! Fetches the block counts of the referenced chunks. Call once #ReadBlocks has drained.
    virtual TFuture<TChunkBlockCounts> GetChunkBlockCounts() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISnapshotReader)

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr CreateSnapshotReader(
    NApi::NNative::IClientPtr client,
    NChunkClient::TUserObject userObject,
    TSnapshotLoadSpec loadSpec,
    TBlockDeviceGeometry geometry,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
