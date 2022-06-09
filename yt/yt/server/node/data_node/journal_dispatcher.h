#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Provides access to changelogs corresponding to journals stored at node.
struct IJournalDispatcher
    : public virtual TRefCounted
{
    //! Asynchronously opens (or returns a cached) changelog corresponding
    //! to a given journal chunk.
    virtual TFuture<NHydra::IFileChangelogPtr> OpenJournal(
        const TStoreLocationPtr& location,
        TChunkId chunkId) = 0;

    //! Asynchronously creates a new changelog corresponding to a given journal chunk.
    virtual TFuture<NHydra::IFileChangelogPtr> CreateJournal(
        const TStoreLocationPtr& location,
        TChunkId chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor) = 0;

    //! Asynchronously removes files of a given journal chunk.
    virtual TFuture<void> RemoveJournal(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing) = 0;

    //! Asynchronously checks if a given journal chunk is sealed.
    virtual TFuture<bool> IsJournalSealed(
        const TStoreLocationPtr& location,
        TChunkId chunkId) = 0;

    //! Asynchronously marks a given journal chunk as sealed.
    virtual TFuture<void> SealJournal(TJournalChunkPtr chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalDispatcher)

IJournalDispatcherPtr CreateJournalDispatcher(
    TDataNodeConfigPtr dataNodeConfig,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
