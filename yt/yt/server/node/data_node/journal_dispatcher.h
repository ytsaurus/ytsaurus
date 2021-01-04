#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/lib/hydra/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Provides access to changelogs corresponding to journals stored at node.
struct IJournalDispatcher
    : public virtual TRefCounted
{
    //! Asynchronously opens (or returns a cached) changelog corresponding
    //! to a given journal chunk.
    virtual TFuture<NHydra::IChangelogPtr> OpenChangelog(
        const TStoreLocationPtr& location,
        TChunkId chunkId) = 0;

    //! Asynchronously creates a new changelog corresponding to a given journal chunk.
    virtual TFuture<NHydra::IChangelogPtr> CreateChangelog(
        const TStoreLocationPtr& location,
        TChunkId chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor) = 0;

    //! Asynchronously removes files of a given journal chunk.
    virtual TFuture<void> RemoveChangelog(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing) = 0;

    //! Asynchronously checks if a given journal chunk is sealed.
    virtual TFuture<bool> IsChangelogSealed(
        const TStoreLocationPtr& location,
        TChunkId chunkId) = 0;

    //! Asynchronously marks a given journal chunk as sealed.
    virtual TFuture<void> SealChangelog(TJournalChunkPtr chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalDispatcher)

IJournalDispatcherPtr CreateJournalDispatcher(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

