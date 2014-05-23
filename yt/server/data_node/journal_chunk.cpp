#include "stdafx.h"
#include "journal_chunk.h"

namespace NYT {
namespace NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TJournalChunk::TJournalChunk(
    TLocationPtr location,
    const TChunkId& chunkId,
    const TChunkMeta& meta,
    const TChunkInfo& info,
    TNodeMemoryTracker* memoryUsageTracker)
    : TChunk(
        location,
        chunkId,
        info,
        memoryUsageTracker)
{
    YUNIMPLEMENTED();
}

IChunk::TAsyncGetMetaResult TJournalChunk::GetMeta(
    i64 priority,
    const std::vector<int>* tags /*= nullptr*/)
{
    YUNIMPLEMENTED();
}

TAsyncError TJournalChunk::ReadBlocks(
    int firstBlockIndex,
    int blockCount,
    i64 priority, std::vector<TSharedRef>* blocks)
{
    YUNIMPLEMENTED();
}

TRefCountedChunkMetaPtr TJournalChunk::GetCachedMeta() const
{
    YUNIMPLEMENTED();
}

void TJournalChunk::DoRemove()
{
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
