#include "memory_writer.h"

#include "deferred_chunk_meta.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <numeric>

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

TFuture<void> TMemoryWriter::Cancel()
{
    return VoidFuture;
}

bool TMemoryWriter::WriteBlock(
    const TWorkloadDescriptor& /*workloadDescriptor*/,
    const TBlock& block)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    Blocks_.push_back(block);
    ChunkInfo_.set_disk_space(ChunkInfo_.disk_space() + block.Size());
    return true;
}

bool TMemoryWriter::WriteBlocks(
    const TWorkloadDescriptor& /*workloadDescriptor*/,
    const std::vector<TBlock>& blocks)
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

TFuture<void> TMemoryWriter::Close(
    const TWorkloadDescriptor& /*workloadDescriptor*/,
    const TDeferredChunkMetaPtr& chunkMeta)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    if (!chunkMeta->IsFinalized()) {
        auto& mapping = chunkMeta->BlockIndexMapping();
        mapping = std::vector<int>(Blocks_.size());
        std::iota(mapping->begin(), mapping->end(), 0);
        chunkMeta->Finalize();
    }

    ChunkMeta_ = chunkMeta;
    Closed_ = true;
    return VoidFuture;
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    return ChunkInfo_;
}

const TDataStatistics& TMemoryWriter::GetDataStatistics() const
{
    YT_UNIMPLEMENTED();
}

TChunkReplicaWithLocationList TMemoryWriter::GetWrittenChunkReplicas() const
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

