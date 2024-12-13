#include "chunk_layout_facade.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/core/misc/checksum.h>

namespace NYT::NChunkClient {

//////////////////////////////////////////////////////////////////////////////

TChunkLayoutFacade::TChunkLayoutFacade(NChunkClient::TChunkId chunkId)
    : ChunkId_(chunkId)
    , Logger(ChunkClientLogger().WithTag("ChunkId: %v", ChunkId_))
{ }

TChunkLayoutFacade::TWriteRequest TChunkLayoutFacade::AddBlocks(const std::vector<TBlock>& blocks)
{
    TWriteRequest request;

    request.StartOffset = DataSize_;
    request.EndOffset = request.StartOffset;

    request.Buffers.reserve(blocks.size());

    for (const auto& block : blocks) {
        auto error = block.ValidateChecksum();
        YT_LOG_FATAL_UNLESS(error.IsOK(), error, "Block checksum mismatch during file writing");

        auto* blockInfo = BlocksExt_.add_blocks();
        blockInfo->set_offset(request.EndOffset);
        blockInfo->set_size(ToProto(block.Size()));
        blockInfo->set_checksum(block.GetOrComputeChecksum());

        request.EndOffset += block.Size();
        request.Buffers.push_back(block.Data);
    }

    DataSize_ = request.EndOffset;

    return request;
}

TSharedMutableRef TChunkLayoutFacade::PrepareChunkMetaBlob()
{
    auto metaData = SerializeProtoToRefWithEnvelope(*ChunkMeta_);

    TChunkMetaHeader_2 header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);
    header.ChunkId = ChunkId_;

    MetaDataSize_ = metaData.Size() + sizeof(header);

    struct TMetaBufferTag
    { };

    auto buffer = TSharedMutableRef::Allocate<TMetaBufferTag>(MetaDataSize_, {.InitializeStorage = false});
    ::memcpy(buffer.Begin(), &header, sizeof(header));
    ::memcpy(buffer.Begin() + sizeof(header), metaData.Begin(), metaData.Size());

    return buffer;
}

TSharedMutableRef TChunkLayoutFacade::Close(TDeferredChunkMetaPtr chunkMeta)
{
    if (!chunkMeta->IsFinalized()) {
        auto& mapping = chunkMeta->BlockIndexMapping();
        mapping = std::vector<int>(BlocksExt_.blocks().size());
        std::iota(mapping->begin(), mapping->end(), 0);
        chunkMeta->Finalize();
    }

    ChunkMeta_->CopyFrom(*chunkMeta);
    SetProtoExtension(ChunkMeta_->mutable_extensions(), BlocksExt_);

    auto chunkMetaBlob = PrepareChunkMetaBlob();

    ChunkInfo_.set_disk_space(DataSize_ + MetaDataSize_);
    return chunkMetaBlob;
}

i64 TChunkLayoutFacade::GetDataSize() const
{
    return DataSize_;
}

i64 TChunkLayoutFacade::GetMetaDataSize() const
{
    return MetaDataSize_;
}

const NChunkClient::TRefCountedChunkMetaPtr& TChunkLayoutFacade::GetChunkMeta() const
{
    return ChunkMeta_;
}


//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient