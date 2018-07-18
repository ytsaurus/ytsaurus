#include "memory_writer.h"

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NChunkClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TMemoryWriter::Open()
{
    YCHECK(!Open_);
    YCHECK(!Closed_);

    Open_ = true;

    return VoidFuture;
}

bool TMemoryWriter::WriteBlock(const TBlock& block)
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    Blocks_.emplace_back(block);
    return true;
}

bool TMemoryWriter::WriteBlocks(const std::vector<TBlock>& blocks)
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    Blocks_.insert(Blocks_.end(), blocks.begin(), blocks.end());
    return true;
}

TFuture<void> TMemoryWriter::GetReadyEvent()
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    return VoidFuture;
}

TFuture<void> TMemoryWriter::Close(const TChunkMeta& chunkMeta)
{
    YCHECK(Open_);
    YCHECK(!Closed_);

    ChunkMeta_ = chunkMeta;
    Closed_ = true;
    return VoidFuture;
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    Y_UNIMPLEMENTED();
}

const TDataStatistics& TMemoryWriter::GetDataStatistics() const
{
    Y_UNIMPLEMENTED();
}

TChunkReplicaList TMemoryWriter::GetWrittenChunkReplicas() const
{
    Y_UNIMPLEMENTED();
}

TChunkId TMemoryWriter::GetChunkId() const
{
    return NullChunkId;
}

NErasure::ECodec TMemoryWriter::GetErasureCodecId() const
{
    return NErasure::ECodec::None;
}

std::vector<TBlock>& TMemoryWriter::GetBlocks()
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

