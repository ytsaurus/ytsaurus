#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <server/hydra/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached journals.
class TJournalDispatcher
    : public TRefCounted
{
public:
    TJournalDispatcher(
        NCellNode::TBootstrap* bootstrap,
        TJournalDispatcherConfigPtr config);
    ~TJournalDispatcher();

    void Initialize();

    //! Returns |true| if new journal chunks are accepted.
    bool AcceptsChunks() const;

    //! Returns a (possibly cached) changelog corresponding to a given journal chunk.
    /*!
     *  This call is thread-safe but may block if the changelog is not cached.
     *  This method throws on failure.
     */
    NHydra::IChangelogPtr OpenChangelog(IChunkPtr chunk);

    //! Creates a new journal chunk corresponding to a given journal session.
    /*!
     *  This call is thread-safe and cannot block. The actual creation happens in background.
     *  This method throws on failure.
     */
    TJournalChunkPtr CreateJournalChunk(const TChunkId& chunkId, TLocationPtr location);

    //! Asynchronously removes a given journal chunk.
    TAsyncError RemoveJournalChunk(IChunkPtr chunk);

    //! Evicts the changelog from the cache.
    void EvictChangelog(IChunkPtr chunk);

private:
    class TCachedChangelog;
    typedef TIntrusivePtr<TCachedChangelog> TCachedChangelogPtr;

    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    class TMultiplexedReplay;

    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

