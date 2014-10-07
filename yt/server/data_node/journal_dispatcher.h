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
        TDataNodeConfigPtr config);
    ~TJournalDispatcher();

    void Initialize();

    //! Returns |true| if new journal chunks are accepted.
    bool AcceptsChunks() const;

    //! Returns a (possibly cached) changelog corresponding to a given journal chunk.
    /*!
     *  This call is thread-safe but may block if the changelog is not cached.
     *  This method throws on failure.
     */
    NHydra::IChangelogPtr OpenChangelog(
        TLocationPtr location,
        const TChunkId& chunkId,
        bool enableMultiplexing);

    //! Creates a new changelog corresponding to a given journal chunk.
    /*!
     *  This call is thread-safe and cannot block. The actual creation happens in background.
     *  This method throws on failure.
     */
    NHydra::IChangelogPtr CreateChangelog(
        IChunkPtr chunk,
        bool enableMultiplexing);

    //! Asynchronously removes files of a given journal chunk.
    TAsyncError RemoveChangelog(IChunkPtr chunk);

    //! Closes and evicts the changelog corresponding to a given journal chunk.
    void CloseChangelog(IChunkPtr chunk);

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

