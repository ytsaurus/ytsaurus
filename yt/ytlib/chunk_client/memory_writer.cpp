#include "memory_writer.h"

#include "chunk_replica.h"

#include <core/actions/future.h>

namespace NYT {
namespace NChunkClient {

using namespace NProto;

///////////////////////////////////////////////////////////////////////////////

TAsyncError TMemoryWriter::Open()
{
    YCHECK(!Open_);
    YCHECK(!Closed_);

    Open_ = true;

    return OKFuture;
}

bool TMemoryWriter::WriteBlock(const TSharedRef& block)
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    Blocks_.emplace_back(block);
    return true;
}

bool TMemoryWriter::WriteBlocks(const std::vector<TSharedRef>& blocks)
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    Blocks_.insert(Blocks_.end(), blocks.begin(), blocks.end());
    return true;
}

TAsyncError TMemoryWriter::GetReadyEvent()
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    return OKFuture;
}

TAsyncError TMemoryWriter::Close(const TChunkMeta& chunkMeta)
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    ChunkMeta_ = chunkMeta;
    Closed_ = true;
    return OKFuture;
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    YUNIMPLEMENTED();
}

TChunkReplicaList TMemoryWriter::GetWrittenChunkReplicas() const
{
    YUNIMPLEMENTED();
}

std::vector<TSharedRef>& TMemoryWriter::GetBlocks()
{
    YCHECK(Open_);
    YCHECK(Closed_);

    return Blocks_;
}

NProto::TChunkMeta& TMemoryWriter::GetChunkMeta()
{
    YCHECK(Open_);
    YCHECK(Closed_);

    return ChunkMeta_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

