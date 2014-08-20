#include "stdafx.h"
#include "versioned_chunk_lookuper.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "versioned_block_reader.h"
#include "versioned_lookuper.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/compression/codec.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

struct TVersionedChunkLookuperPoolTag { };

template <class TBlockReader>
class TVersionedChunkLookuper
    : public IVersionedLookuper
{
public:
    TVersionedChunkLookuper(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IReaderPtr chunkReader,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp)
        : Config_(std::move(config))
        , ChunkMeta_(std::move(chunkMeta))
        , ChunkReader_(std::move(chunkReader))
        , Timestamp_(timestamp)
        , MemoryPool_(TVersionedChunkLookuperPoolTag())
    {
        YCHECK(ChunkMeta_->Misc().sorted());
        YCHECK(ChunkMeta_->ChunkMeta().type() == EChunkType::Table);
        YCHECK(ChunkMeta_->ChunkMeta().version() == TBlockReader::FormatVersion);
        YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);

        if (columnFilter.All) {
            SchemaIdMapping_ = ChunkMeta_->SchemaIdMapping();
        } else {
            SchemaIdMapping_.reserve(ChunkMeta_->SchemaIdMapping().size());
            int keyColumnCount = static_cast<int>(ChunkMeta_->KeyColumns().size());
            for (auto index : columnFilter.Indexes) {
                if (index >= keyColumnCount) {
                    auto mappingIndex = index - keyColumnCount;
                    SchemaIdMapping_.push_back(ChunkMeta_->SchemaIdMapping()[mappingIndex]);
                }
            }
        }
    }

    virtual TFuture<TErrorOr<TVersionedRow>> Lookup(TKey key) override
    {
        if (key < ChunkMeta_->GetMinKey().Get() || key > ChunkMeta_->GetMaxKey().Get()) {
            static auto NullRow = MakeFuture<TErrorOr<TVersionedRow>>(TVersionedRow());
            return NullRow;
        }

        int blockIndex = GetBlockIndex(key);

        PooledBlockIndexes_.clear();
        PooledBlockIndexes_.push_back(blockIndex);

        auto asyncResult = ChunkReader_->ReadBlocks(PooledBlockIndexes_);
        return asyncResult.Apply(
            BIND(&TVersionedChunkLookuper::OnBlockRead, MakeStrong(this), key, blockIndex)
                .AsyncVia(TDispatcher::Get()->GetCompressionPoolInvoker()));
    }


private:
    TChunkReaderConfigPtr Config_;
    TCachedVersionedChunkMetaPtr ChunkMeta_;
    IReaderPtr ChunkReader_;
    TTimestamp Timestamp_;

    std::vector<TColumnIdMapping> SchemaIdMapping_;

    TChunkedMemoryPool MemoryPool_;

    std::vector<int> PooledBlockIndexes_;


    int GetBlockIndex(TKey key)
    {
        const auto& blockIndexKeys = ChunkMeta_->BlockIndexKeys();

        typedef decltype(blockIndexKeys.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexKeys.end());
        auto rend = std::reverse_iterator<TIter>(blockIndexKeys.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            key,
            [] (TKey pivot, const TOwningKey& indexKey) {
                return pivot > indexKey.Get();
            });

        if (it == rend) {
            return 0;
        } else {
            return std::distance(it, rend);
        }
    }

    TErrorOr<TVersionedRow> OnBlockRead(
        TKey key,
        int blockIndex,
        NChunkClient::IReader::TReadBlocksResult result)
    {
        if (!result.IsOK()) {
            return TError(result);
        }

        const auto& compressedBlocks = result.Value();
        YASSERT(compressedBlocks.size() == 1);

        const auto& compressedBlock = compressedBlocks[0];
        auto* codec = GetCodec(ECodec(ChunkMeta_->Misc().compression_codec()));
        auto decompressedBlock = codec->Decompress(compressedBlock);

        TBlockReader blockReader(
            std::move(decompressedBlock),
            ChunkMeta_->BlockMeta().entries(blockIndex),
            ChunkMeta_->ChunkSchema(),
            ChunkMeta_->KeyColumns(),
            SchemaIdMapping_,
            Timestamp_);

        if (!blockReader.SkipToKey(key)) {
            return TVersionedRow();
        }

        if (blockReader.GetKey() != key) {
            return TVersionedRow();
        }

        return blockReader.GetRow(&MemoryPool_);
    }

};

IVersionedLookuperPtr CreateVersionedChunkLookuper(
    TChunkReaderConfigPtr config,
    IReaderPtr chunkReader,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    switch (chunkMeta->ChunkMeta().version()) {
        case ETableChunkFormat::VersionedSimple:
            return New<TVersionedChunkLookuper<TSimpleVersionedBlockReader>>(
                std::move(config),
                std::move(chunkMeta),
                std::move(chunkReader),
                columnFilter,
                timestamp);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
