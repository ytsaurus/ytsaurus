#pragma once

#include "common.h"

#include "../actions/action_queue.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

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

    TChunkId GetId() const
    {
        return Id;
    }

    i64 GetSize() const
    {
        return Size;
    }

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

class TChunkStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkStore> TPtr;

    TChunkStore(const TChunkHolderConfig& config);

    TChunk::TPtr RegisterChunk(
        const TChunkId& chunkId,
        i64 size,
        int location);
    
    TChunk::TPtr FindChunk(const TChunkId& chunkId);

    IInvoker::TPtr GetIOInvoker(int location);

    int GetNewChunkLocation(const TChunkId& chunkId);

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

