#include "memory_writer.h"

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TMemoryWriter::Open()
{
    YT_VERIFY(!Open_);
    YT_VERIFY(!Closed_);

    Open_ = true;

    return VoidFuture;
}

bool TMemoryWriter::WriteBlock(const TBlock& block)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    Blocks_.emplace_back(block);
    return true;
}

bool TMemoryWriter::WriteBlocks(const std::vector<TBlock>& blocks)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    Blocks_.insert(Blocks_.end(), blocks.begin(), blocks.end());
    return true;
}

TFuture<void> TMemoryWriter::GetReadyEvent()
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    return VoidFuture;
}

TFuture<void> TMemoryWriter::Close(const TRefCountedChunkMetaPtr& chunkMeta)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    ChunkMeta_ = chunkMeta;
    Closed_ = true;
    return VoidFuture;
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    YT_UNIMPLEMENTED();
}

const TDataStatistics& TMemoryWriter::GetDataStatistics() const
{
    YT_UNIMPLEMENTED();
}

TChunkReplicaWithMediumList TMemoryWriter::GetWrittenChunkReplicas() const
{
    YT_UNIMPLEMENTED();
}

bool TMemoryWriter::IsCloseDemanded() const
{
    YT_UNIMPLEMENTED();
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
    YT_VERIFY(Open_);
    YT_VERIFY(Closed_);

    return Blocks_;
}

TRefCountedChunkMetaPtr TMemoryWriter::GetChunkMeta()
{
    YT_VERIFY(Open_);
    YT_VERIFY(Closed_);

    return ChunkMeta_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

