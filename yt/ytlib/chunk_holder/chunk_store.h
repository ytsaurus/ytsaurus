#pragma once

#include "common.h"

#include "../actions/action_queue.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Describes an already uploaded chunk.
class TChunk
    : public TRefCountedBase
{
public:
    TChunk(
        const TChunkId& id,
        i64 size,
        int location)
        : Id(id)
        , Size(size)
        , Location(location)
    { }

    typedef TIntrusivePtr<TChunk> TPtr;

    //! Returns chunk id.
    TChunkId GetId() const
    {
        return Id;
    }

    //! Returns chunk size.
    i64 GetSize() const
    {
        return Size;
    }

    //! Returns chunk storage location.
    int GetLocation() const
    {
        return Location;
    }

private:
    TChunkId Id;
    i64 Size;
    int Location;

};

////////////////////////////////////////////////////////////////////////////////

//! Manages uploaded chunks.
class TChunkStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkStore> TPtr;

    //! Constructs a new instance.
    TChunkStore(const TChunkHolderConfig& config);

    //! Registers a just-uploaded chunk for further usage.
    TChunk::TPtr RegisterChunk(
        const TChunkId& chunkId,
        i64 size,
        int location);
    
    //! Finds chunk by id. Returns NULL if no chunk exists.
    TChunk::TPtr FindChunk(const TChunkId& chunkId);

    //! Returns invoker for a given storage location.
    IInvoker::TPtr GetIOInvoker(int location);

    //! Calculates a storage location for a new chunk.
    int GetNewChunkLocation(const TChunkId& chunkId);

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const TChunkId& chunkId, int location);

private:
    void ScanChunks();
    void InitIOQueues();

    TChunkHolderConfig Config; // TODO: avoid copying

    //! Actions queues that handle IO requests to chunk storage locations.
    yvector<IInvoker::TPtr> IOInvokers;

    typedef yhash_map<TChunkId, TChunk::TPtr, TGUIDHash> TChunkMap;
    TChunkMap ChunkMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

