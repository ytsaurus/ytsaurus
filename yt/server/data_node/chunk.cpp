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

} // namespace NDataNode
} // namespace NYT
