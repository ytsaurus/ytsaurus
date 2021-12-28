#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/ref.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages journal chunks stored at some specific location.
class TJournalManager
    : public TRefCounted
{
public:
    TJournalManager(
        TDataNodeConfigPtr config,
        TStoreLocation* location,
        TChunkHostPtr chunkHost);
    ~TJournalManager();

    void Initialize();

    TFuture<NHydra::IChangelogPtr> OpenChangelog(
        TChunkId chunkId);

    TFuture<NHydra::IChangelogPtr> CreateChangelog(
        TChunkId chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor);

    TFuture<void> RemoveChangelog(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing);

    // Returns true if some records were skipped.
    TFuture<bool> AppendMultiplexedRecords(
        TChunkId chunkId,
        int firstRecordId,
        TRange<TSharedRef> records,
        TFuture<void> splitResult);

    TFuture<bool> IsChangelogSealed(TChunkId chunkId);

    TFuture<void> SealChangelog(const TJournalChunkPtr& chunk);

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TJournalManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

