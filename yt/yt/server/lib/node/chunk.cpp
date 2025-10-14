#include "chunk.h"

#include "chunk_location.h"

namespace NYT::NNode {

////////////////////////////////////////////////////////////////////////////////

TChunkDescriptor::TChunkDescriptor(TChunkId id, i64 diskSpace)
    : Id(id)
    , DiskSpace(diskSpace)
{ }

////////////////////////////////////////////////////////////////////////////////

TLockedChunkGuard::TLockedChunkGuard(TChunkLocationBasePtr location, TChunkId chunkId)
    : Location_(std::move(location))
    , ChunkId_(chunkId)
{ }

TLockedChunkGuard::TLockedChunkGuard(TLockedChunkGuard&& other)
{
    MoveFrom(std::move(other));
}

TLockedChunkGuard::~TLockedChunkGuard()
{
    if (Location_) {
        Location_->UnlockChunk(ChunkId_);
    }
}

TLockedChunkGuard& TLockedChunkGuard::operator=(TLockedChunkGuard&& other)
{
    if (this != &other) {
        MoveFrom(std::move(other));
    }
    return *this;
}

void TLockedChunkGuard::Release()
{
    Location_.Reset();
    ChunkId_ = {};
}

void TLockedChunkGuard::MoveFrom(TLockedChunkGuard&& other)
{
    Location_ = std::move(other.Location_);
    ChunkId_ = other.ChunkId_;

    other.Location_.Reset();
    other.ChunkId_ = {};
}

TLockedChunkGuard::operator bool() const
{
    return Location_.operator bool();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNode
