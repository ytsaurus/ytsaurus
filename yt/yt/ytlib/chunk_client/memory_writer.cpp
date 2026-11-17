#include "memory_writer.h"

#include "chunk_meta_extensions.h"
#include "deferred_chunk_meta.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <numeric>

namespace NYT::NChunkClient {

using namespace NObjectClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TMemoryWriter::TMemoryWriter(TChunkId chunkId)
    : ChunkId_(chunkId)
{
    if (chunkId != NullChunkId && TypeFromId(chunkId) != EObjectType::Chunk) {
        THROW_ERROR_EXCEPTION("Memory writer requires an ordinary chunk ID")
            .With("chunk_id", chunkId)
            .With("chunk_type", TypeFromId(chunkId));
    }
}

TFuture<void> TMemoryWriter::Open()
{
    YT_VERIFY(!Open_);
    YT_VERIFY(!Closed_);

    Open_ = true;

    return OKFuture;
}

TFuture<void> TMemoryWriter::Cancel()
{
    return OKFuture;
}

bool TMemoryWriter::WriteBlock(
    const IChunkWriter::TWriteBlocksOptions& /*options*/,
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
    const IChunkWriter::TWriteBlocksOptions& /*options*/,
    const TWorkloadDescriptor& /*workloadDescriptor*/,
    const std::vector<TBlock>& blocks)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    Blocks_.insert(Blocks_.end(), blocks.begin(), blocks.end());

    for (const auto& block : blocks) {
        ChunkInfo_.set_disk_space(ChunkInfo_.disk_space() + block.Size());
    }

    return true;
}

TFuture<void> TMemoryWriter::GetReadyEvent()
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    return OKFuture;
}

TFuture<void> TMemoryWriter::Close(
    const IChunkWriter::TWriteBlocksOptions& /*options*/,
    const TWorkloadDescriptor& /*workloadDescriptor*/,
    const TDeferredChunkMetaPtr& chunkMeta)
{
    YT_VERIFY(Open_);
    YT_VERIFY(!Closed_);

    try {
        if (!chunkMeta) {
            THROW_ERROR_EXCEPTION("Memory writer requires chunk metadata");
        }

        if (!chunkMeta->IsFinalized()) {
            auto& mapping = chunkMeta->BlockIndexMapping();
            mapping = std::vector<int>(Blocks_.size());
            std::iota(mapping->begin(), mapping->end(), 0);
            chunkMeta->Finalize();
        }

        FinalizeStatistics(chunkMeta);
        ChunkMeta_ = chunkMeta;
        Closed_ = true;
        return OKFuture;
    } catch (const std::exception& ex) {
        return MakeFuture<void>(TError(ex));
    }
}

const TChunkInfo& TMemoryWriter::GetChunkInfo() const
{
    return ChunkInfo_;
}

const TDataStatistics& TMemoryWriter::GetDataStatistics() const
{
    YT_VERIFY(Closed_);

    return Statistics_;
}

TWrittenChunkReplicasInfo TMemoryWriter::GetWrittenChunkReplicasInfo() const
{
    YT_VERIFY(Closed_);

    return {};
}

bool TMemoryWriter::IsCloseDemanded() const
{
    return false;
}

TChunkId TMemoryWriter::GetChunkId() const
{
    return ChunkId_;
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

void TMemoryWriter::FinalizeStatistics(const TDeferredChunkMetaPtr& chunkMeta)
{
    i64 diskSpace = ChunkInfo_.disk_space();
    Statistics_.set_chunk_count(1);
    Statistics_.set_regular_disk_space(diskSpace);
    Statistics_.set_compressed_data_size(diskSpace);

    auto miscExt = FindProtoExtension<TMiscExt>(chunkMeta->extensions());
    if (!miscExt) {
        return;
    }

    auto compressedSize = miscExt->has_compressed_data_size()
        ? miscExt->compressed_data_size()
        : diskSpace;
    if (miscExt->row_count() < 0 ||
        miscExt->data_weight() < 0 ||
        miscExt->uncompressed_data_size() < 0 ||
        compressedSize < 0 ||
        miscExt->erasure_codec() != static_cast<int>(NErasure::ECodec::None))
    {
        THROW_ERROR_EXCEPTION("Memory writer requires nonnegative non-erasure chunk statistics")
            .With("row_count", miscExt->row_count())
            .With("data_weight", miscExt->data_weight())
            .With("uncompressed_data_size", miscExt->uncompressed_data_size())
            .With("compressed_data_size", compressedSize)
            .With("erasure_codec", miscExt->erasure_codec());
    }

    // TEncodingWriter estimates this size via a floating-point compression ratio,
    // even after flush; truncation can lose one byte per input chunk.
    // Metadata aggregation can accumulate these rounding losses.
    bool sizeIsConsistent = compressedSize <= diskSpace;
    if (!sizeIsConsistent) {
        THROW_ERROR_EXCEPTION("Memory writer block size disagrees with compressed metadata size")
            .With("block_size", diskSpace)
            .With("compressed_data_size", compressedSize);
    }

    Statistics_.set_row_count(miscExt->row_count());
    Statistics_.set_data_weight(miscExt->data_weight());
    Statistics_.set_uncompressed_data_size(miscExt->uncompressed_data_size());

    if (miscExt->has_compressed_data_size()) {
        Statistics_.set_compressed_data_size(compressedSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
