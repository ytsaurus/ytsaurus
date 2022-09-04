#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages journal chunks stored at some specific location.
struct IJournalManager
    : public TRefCounted
{
    virtual void Initialize() = 0;

    virtual void Reconfigure(
        TJournalManagerConfigPtr config) = 0;

    virtual TFuture<NHydra::IFileChangelogPtr> OpenChangelog(
        TChunkId chunkId) = 0;

    virtual TFuture<NHydra::IFileChangelogPtr> CreateChangelog(
        TChunkId chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor)  = 0;

    virtual TFuture<void> RemoveChangelog(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing) = 0;

    // Returns true if some records were skipped.
    virtual TFuture<bool> AppendMultiplexedRecords(
        TChunkId chunkId,
        int firstRecordId,
        TRange<TSharedRef> records,
        TFuture<void> splitResult)  = 0;

    virtual TFuture<bool> IsChangelogSealed(TChunkId chunkId) = 0;

    virtual TFuture<void> SealChangelog(const TJournalChunkPtr& chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalManager)

////////////////////////////////////////////////////////////////////////////////

IJournalManagerPtr CreateJournalManager(
    TJournalManagerConfigPtr config,
    TStoreLocation* location,
    TChunkContextPtr chunkContext,
    INodeMemoryTrackerPtr nodeMemoryTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

