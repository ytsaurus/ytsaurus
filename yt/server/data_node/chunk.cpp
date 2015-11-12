#include "chunk.h"
#include "journal_chunk.h"

#include <yt/ytlib/chunk_client/chunk_replica.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

EObjectType IChunk::GetType() const
{
    return TypeFromId(DecodeChunkId(GetId()).Id);
}

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
    return Chunk_.operator bool();
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
