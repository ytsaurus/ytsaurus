#pragma once

#include "common.h"
#include "config.h"

#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunk;
class TReaderCache;
struct TChunkDescriptor;

DECLARE_ENUM(ELocationType,
    (Store)
    (Cache)
);

//! Describes a physical location of chunks at a chunk holder.
class TLocation
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TLocation> TPtr;

    TLocation(
        ELocationType type,
        TLocationConfig* config,
        TReaderCache* readerCache,
        const Stroka& threadName);

    //! Returns the type.
    ELocationType GetType() const;

    //! Scan the location directory removing orphaned files and returning the list of found chunks.
    yvector<TChunkDescriptor> Scan();

    //! Updates #UsedSpace and #AvailalbleSpace
    void RegisterChunk(TChunk* chunk);

    //! Updates #UsedSpace and #AvailalbleSpace
    void UnregisterChunk(TChunk* chunk);

    //! Schedules physical removal of a chunk.
    void RemoveChunk(TChunk* chunk);

    //! Updates #AvailalbleSpace with a system call and returns the result.
    i64 GetAvailableSpace();

    //! Returns the invoker that handles all IO requests to this location.
    IInvoker::TPtr GetInvoker() const;

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

    //! Increments the number of currently active sessions.
    void IncrementSessionCount();

    //! Decrements the number of currently active sessions.
    void DecrementSessionCount();

    //! Returns the number of currently active sessions.
    int GetSessionCount() const;

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const NChunkClient::TChunkId& chunkId) const;

private:
    ELocationType Type;
    TLocationConfig::TPtr Config;
    TIntrusivePtr<TReaderCache> ReaderCache;
    i64 AvailableSpace;
    i64 UsedSpace;
    TActionQueue::TPtr ActionQueue;
    int SessionCount;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

