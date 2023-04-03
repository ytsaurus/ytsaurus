#include "preloaded_block_cache.h"
#include "chunk_reader.h"
#include "ref_counted_proto.h"
#include "chunk_meta_extensions.h"
#include "block_cache.h"
#include "chunk_reader_options.h"

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/compression/codec.h>

#include <library/cpp/yt/misc/cast.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TPreloadedBlockCache
    : public IBlockCache
{
public:
    TPreloadedBlockCache(
        TChunkId chunkId,
        const std::vector<NChunkClient::TBlock>& blocks)
        : ChunkId_(chunkId)
        , Blocks_(blocks)
    { }

    void PutBlock(
        const NChunkClient::TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*data*/) override
    { }

    TCachedBlock FindBlock(
        const NChunkClient::TBlockId& id,
        EBlockType /*type*/) override
    {
        YT_ASSERT(id.ChunkId == ChunkId_);
        return TCachedBlock{Blocks_[id.BlockIndex]};
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::UncompressedData;
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const NChunkClient::TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return nullptr;
    }

private:
    const TChunkId ChunkId_;
    const std::vector<NChunkClient::TBlock> Blocks_;
};

IBlockCachePtr GetPreloadedBlockCache(IChunkReaderPtr chunkReader)
{
    auto meta = NConcurrency::WaitFor(chunkReader->GetMeta(/*chunkReadOptions*/ {}))
        .ValueOrThrow();

    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
    auto blockMetaExt = GetProtoExtension<NTableClient::NProto::TDataBlockMetaExt>(meta->extensions());

    auto compressedBlocks = NConcurrency::WaitFor(chunkReader->ReadBlocks(
        /*options*/ {},
        0,
        blockMetaExt.data_blocks_size()))
        .ValueOrThrow();

    auto codecId = CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec());
    auto* codec = NCompression::GetCodec(codecId);

    std::vector<TBlock> cachedBlocks;
    for (const auto& compressedBlock : compressedBlocks) {
        cachedBlocks.emplace_back(codec->Decompress(compressedBlock.Data));
    }

    return New<TPreloadedBlockCache>(chunkReader->GetChunkId(), std::move(cachedBlocks));
}

IBlockCachePtr GetPreloadedBlockCache(TChunkId chunkId, const std::vector<NChunkClient::TBlock>& blocks)
{
    return New<TPreloadedBlockCache>(chunkId, blocks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
