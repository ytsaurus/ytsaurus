#include "stdafx.h"
#include "chunk.h"
#include "journal_chunk.h"

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

TRefCountedChunkMeta::TRefCountedChunkMeta()
{ }

TRefCountedChunkMeta::TRefCountedChunkMeta(const TRefCountedChunkMeta& other)
{
    CopyFrom(other);
}

TRefCountedChunkMeta::TRefCountedChunkMeta(TRefCountedChunkMeta&& other)
{
    Swap(&other);
}

TRefCountedChunkMeta::TRefCountedChunkMeta(const NChunkClient::NProto::TChunkMeta& other)
{
    CopyFrom(other);
}

TRefCountedChunkMeta::TRefCountedChunkMeta(NChunkClient::NProto::TChunkMeta&& other)
{
    Swap(&other);
}

////////////////////////////////////////////////////////////////////////////////

TJournalChunkPtr IChunk::AsJournalChunk()
{
    auto* journalChunk = dynamic_cast<TJournalChunk*>(this);
    YCHECK(journalChunk);
    return journalChunk;
}

////////////////////////////////////////////////////////////////////////////////

TChunkReadGuard::TChunkReadGuard(IChunkPtr chunk)
    : Chunk_(chunk)
{ }

TChunkReadGuard& TChunkReadGuard::operator=(TChunkReadGuard&& other)
{
    swap(*this, other);
    return *this;
}

TChunkReadGuard::~TChunkReadGuard()
{
    if (Chunk_) {
        Chunk_->ReleaseReadLock();
    }
}

TChunkReadGuard::operator bool() const
{
    return Chunk_ != nullptr;
}

void swap(TChunkReadGuard& lhs, TChunkReadGuard& rhs)
{
    using std::swap;
    swap(lhs.Chunk_, rhs.Chunk_);
}

TChunkReadGuard TChunkReadGuard::TryAcquire(IChunkPtr chunk)
{
    return chunk->TryAcquireReadLock()
        ? TChunkReadGuard(chunk)
        : TChunkReadGuard();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
