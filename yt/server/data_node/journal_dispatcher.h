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
     *  This call is thread-safe but may block since it actually opens the files.
     *  This method throws on failure.
     */
    NHydra::IChangelogPtr GetChangelog(IChunkPtr chunk);

    //! Creates a new changelog corresponding to a given journal session.
    /*!
     *  This call is thread-safe but may block since it actually creates the files.
     *  This method throws on failure.
     */
    NHydra::IChangelogPtr CreateChangelog(ISessionPtr session);

    //! Evicts the changelog from the cache.
    void EvictChangelog(IChunkPtr chunk);

private:
    class TCachedChangelog;
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

