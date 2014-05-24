#include "memory_writer.h"

#include <core/actions/future.h>

namespace NYT {
namespace NChunkClient {

using namespace NProto;

///////////////////////////////////////////////////////////////////////////////

TMemoryWriter::TMemoryWriter()
    : IsClosed_(false)
{ }

void TMemoryWriter::Open()
{ }

bool TMemoryWriter::WriteBlock(const TSharedRef& block)
{
    YCHECK(!IsClosed_);
    Blocks_.emplace_back(block);
    return true;
}

TAsyncError TMemoryWriter::GetReadyEvent()
{
    YCHECK(!IsClosed_);
    return OKFuture;
}

TAsyncError TMemoryWriter::Close(const TChunkMeta& chunkMeta)
{
    YCHECK(!IsClosed_);
    ChunkMeta_ = chunkMeta;
    IsClosed_ = true;
    return OKFuture;
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    YUNIMPLEMENTED();
}

IWriter::TReplicaIndexes TMemoryWriter::GetWrittenReplicaIndexes() const
{
    YUNIMPLEMENTED();
}

std::vector<TSharedRef>& TMemoryWriter::GetBlocks()
{
    YCHECK(IsClosed_);
    return Blocks_;
}

NProto::TChunkMeta& TMemoryWriter::GetChunkMeta()
{
    YCHECK(IsClosed_);
    return ChunkMeta_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

