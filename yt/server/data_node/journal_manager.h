#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/hydra/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref.h>

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
        NCellNode::TBootstrap* bootstrap);
    ~TJournalManager();

    void Initialize();

    TFuture<NHydra::IChangelogPtr> OpenChangelog(
        const TChunkId& chunkId);

    TFuture<NHydra::IChangelogPtr> CreateChangelog(
        const TChunkId& chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor);

    TFuture<void> RemoveChangelog(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing);

    TFuture<void> AppendMultiplexedRecord(
        const TChunkId& chunkId,
        int recordId,
        const TSharedRef& recordData,
        TFuture<void> splitResult);

    TFuture<bool> IsChangelogSealed(const TChunkId& chunkId);

    TFuture<void> SealChangelog(const TJournalChunkPtr& chunk);

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

