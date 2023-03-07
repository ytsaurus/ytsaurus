#include "chunk.h"
#include "journal_chunk.h"

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

EObjectType IChunk::GetType() const
{
    return TypeFromId(DecodeChunkId(GetId()).Id);
}

bool IChunk::IsJournalChunk() const
{
    return IsJournalChunkId(DecodeChunkId(GetId()).Id);
}

TJournalChunkPtr IChunk::AsJournalChunk()
{
    auto* journalChunk = dynamic_cast<TJournalChunk*>(this);
    YT_VERIFY(journalChunk);
    return journalChunk;
}

////////////////////////////////////////////////////////////////////////////////

TChunkReadGuard::TChunkReadGuard(IChunkPtr chunk)
    : Chunk_(std::move(chunk))
{ }

TChunkReadGuard::~TChunkReadGuard()
{
    if (Chunk_) {
        Chunk_->ReleaseReadLock();
    }
}

TChunkReadGuard::operator bool() const
{
    return Chunk_.operator bool();
}

TChunkReadGuard TChunkReadGuard::Acquire(IChunkPtr chunk)
{
    chunk->AcquireReadLock();
    return TChunkReadGuard(std::move(chunk));
}

////////////////////////////////////////////////////////////////////////////////

TChunkUpdateGuard::TChunkUpdateGuard(IChunkPtr chunk)
    : Chunk_(std::move(chunk))
{ }

TChunkUpdateGuard::~TChunkUpdateGuard()
{
    if (Chunk_) {
        Chunk_->ReleaseUpdateLock();
    }
}

TChunkUpdateGuard::operator bool() const
{
    return Chunk_.operator bool();
}

TChunkUpdateGuard TChunkUpdateGuard::Acquire(IChunkPtr chunk)
{
    chunk->AcquireUpdateLock();
    return TChunkUpdateGuard(std::move(chunk));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
