#pragma once

#include "public.h"

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
    TJournalDispatcher(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TJournalDispatcher();

    //! Asynchronously opens (or returns a cached) changelog corresponding
    //! to a given journal chunk.
    TFuture<NHydra::IChangelogPtr> OpenChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    //! Asynchronously creates a new changelog corresponding to a given journal chunk.
    TFuture<NHydra::IChangelogPtr> CreateChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    //! Asynchronously removes files of a given journal chunk.
    TFuture<void> RemoveChangelog(TJournalChunkPtr chunk);

private:
    class TCachedChangelog;
    typedef TIntrusivePtr<TCachedChangelog> TCachedChangelogPtr;

    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

