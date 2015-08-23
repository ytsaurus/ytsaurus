#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <server/hydra/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

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
        TStoreLocationPtr location,
        const TChunkId& chunkId);

    //! Asynchronously creates a new changelog corresponding to a given journal chunk.
    TFuture<NHydra::IChangelogPtr> CreateChangelog(
        TStoreLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    //! Asynchronously removes files of a given journal chunk.
    TFuture<void> RemoveChangelog(
        TJournalChunkPtr chunk,
        bool enableMultiplexing);

    //! Asynchronously checks if a given journal chunk is sealed.
    TFuture<bool> IsChangelogSealed(
        TStoreLocationPtr location,
        const TChunkId& chunkId);

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

} // namespace NDataNode
} // namespace NYT

