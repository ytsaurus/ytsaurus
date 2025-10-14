#pragma once

#include "public.h"

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained during the filesystem scan.
struct TChunkDescriptor
{
    TChunkDescriptor() = default;

    explicit TChunkDescriptor(TChunkId id, i64 diskSpace = 0);

    TChunkId Id;
    i64 DiskSpace = 0;

    // For journal chunks only.
    i64 RowCount = 0;
    bool Sealed = false;
};

////////////////////////////////////////////////////////////////////////////////

class TLockedChunkGuard
{
public:
    TLockedChunkGuard() = default;
    TLockedChunkGuard(TLockedChunkGuard&& other);
    ~TLockedChunkGuard();

    //! This method loses pointer to location and chunk for exclude
    //! Location::UnlockChunk call in destructor. This is necessary to preserve
    //! eternal (while the location is alive) lock on the chunk.
    void Release();

    TLockedChunkGuard& operator=(TLockedChunkGuard&& other);

    explicit operator bool() const;

private:
    friend class TChunkLocationBase;

    TLockedChunkGuard(TChunkLocationBasePtr location, TChunkId chunkId);

    void MoveFrom(TLockedChunkGuard&& other);

    TChunkLocationBasePtr Location_;
    TChunkId ChunkId_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
