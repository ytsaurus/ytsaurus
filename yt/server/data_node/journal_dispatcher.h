#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/hydra/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Provides access to changelogs corresponding to journals stored at node.
class TJournalDispatcher
    : public TRefCounted
{
public:
    explicit TJournalDispatcher(TDataNodeConfigPtr config);
    ~TJournalDispatcher();

    //! Asynchronously opens (or returns a cached) changelog corresponding
    //! to a given journal chunk.
    TFuture<NHydra::IChangelogPtr> OpenChangelog(
        const TStoreLocationPtr& location,
        TChunkId chunkId);

    //! Asynchronously creates a new changelog corresponding to a given journal chunk.
    TFuture<NHydra::IChangelogPtr> CreateChangelog(
        const TStoreLocationPtr& location,
        TChunkId chunkId,
        bool enableMultiplexing,
        const TWorkloadDescriptor& workloadDescriptor);

    //! Asynchronously removes files of a given journal chunk.
    TFuture<void> RemoveChangelog(
        const TJournalChunkPtr& chunk,
        bool enableMultiplexing);

    //! Asynchronously checks if a given journal chunk is sealed.
    TFuture<bool> IsChangelogSealed(
        const TStoreLocationPtr& location,
        TChunkId chunkId);

    //! Asynchronously marks a given journal chunk as sealed.
    TFuture<void> SealChangelog(TJournalChunkPtr chunk);

private:
    struct TCachedChangelogKey;

    class TCachedChangelog;
    using TCachedChangelogPtr = TIntrusivePtr<TCachedChangelog>;

    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    const TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

