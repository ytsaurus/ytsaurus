#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////


DECLARE_ENUM(ELocationType,
    (Store)
    (Cache)
);

//! Describes a physical location of chunks at a chunk holder.
class TLocation
    : public TRefCounted
{
public:
    TLocation(
        ELocationType type,
        TLocationConfigPtr config,
        TReaderCachePtr readerCache,
        const Stroka& threadName);

    ~TLocation();

    //! Returns the type.
    ELocationType GetType() const;

    //! Scan the location directory removing orphaned files and returning the list of found chunks.
    std::vector<TChunkDescriptor> Scan();

    //! Updates #UsedSpace and #AvailalbleSpace
    void UpdateUsedSpace(i64 size);

    //! Schedules physical removal of a chunk.
    void RemoveChunk(TChunkPtr chunk);

    //! Updates #AvailalbleSpace with a system call and returns the result.
    i64 GetAvailableSpace() const;

    //! Returns the invoker that handles all IO requests to this location.
    IInvokerPtr GetInvoker() const;

    //! Returns the reader cache.
    TIntrusivePtr<TReaderCache> GetReaderCache() const;

    //! Returns the number of bytes used at the location.
    /*!
     *  \note
     *  This may exceed #GetQuota.
     */
    i64 GetUsedSpace() const;

    //! Returns the maximum number of bytes the chunks assigned to this location
    //! are allowed to use.
    i64 GetQuota() const;

    //! Returns the path of the location.
    Stroka GetPath() const;

    //! Returns the load factor.
    double GetLoadFactor() const;

    //! Changes the number of currently active sessions by a given delta.
    void UpdateSessionCount(int delta);

    //! Returns the number of currently active sessions.
    int GetSessionCount() const;

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const TChunkId& chunkId) const;

    //! Checks whether the location is full.
    bool IsFull() const;

    //! Checks whether to location has enough space to contain file of size #size
    bool HasEnoughSpace(i64 size) const;

private:
    ELocationType Type;
    TLocationConfigPtr Config;
    TReaderCachePtr ReaderCache;
    mutable i64 AvailableSpace;
    i64 UsedSpace;
    TActionQueue::TPtr ActionQueue;
    int SessionCount;

    mutable NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

