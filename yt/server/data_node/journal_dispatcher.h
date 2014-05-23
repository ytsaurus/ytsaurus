#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached journals.
class TJournalDispatcher
    : public TRefCounted
{
public:
    TJournalDispatcher(
        TDataNodeConfigPtr config,
        const Stroka& threadName);
    ~TJournalDispatcher();

    //! Returns a (cached) changelog corresponding to a given journal chunk.
    /*!
     *  This call is thread-safe but may block since it actually opens the journal.
     *  A rule of thumb is to invoke it from IO thread only.
     *
     *  This method throws on failure.
     */
    NHydra::IChangelogPtr GetChangelog(TJournalChunkPtr chunk);

    //! Evicts the changelog from the cache.
    void EvictChangelog(TJournalChunkPtr chunk);

private:
    class TCachedJournal;
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

