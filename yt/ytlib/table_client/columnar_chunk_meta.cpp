#include "columnar_chunk_meta.h"
#include "row_buffer.h"
#include "name_table.h"

namespace NYT {
namespace NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkMeta::TColumnarChunkMeta(const TChunkMeta& chunkMeta)
{
    InitExtensions(chunkMeta);
}

void TColumnarChunkMeta::InitExtensions(const TChunkMeta& chunkMeta)
{
    ChunkType_ = EChunkType(chunkMeta.type());
    ChunkFormat_ = ETableChunkFormat(chunkMeta.version());

    Misc_ = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
    BlockMeta_ = New<TRefCountedBlockMeta>(GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions()));

    // This is for old horizontal versioned chunks, since TCachedVersionedChunkMeta use this call.
    if (auto columnMeta = FindProtoExtension<TColumnMetaExt>(chunkMeta.extensions())) {
        ColumnMeta_ = New<TRefCountedColumnMeta>(std::move(*columnMeta));
    }

    auto keyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
    auto tableSchemaExt = FindProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
    if (tableSchemaExt && keyColumnsExt) {
        FromProto(&ChunkSchema_, *tableSchemaExt, *keyColumnsExt);
    } else if (tableSchemaExt) {
        FromProto(&ChunkSchema_, *tableSchemaExt);
    } else if (keyColumnsExt) {
        // COMPAT(savrus) No table schema is allowed only for old chunks.
        YCHECK(ChunkFormat_ == ETableChunkFormat::SchemalessHorizontal);
        TKeyColumns keyColumns = NYT::FromProto<TKeyColumns>(*keyColumnsExt);
        ChunkSchema_ = TTableSchema::FromKeyColumns(keyColumns);
    }

    if (auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions())) {
        ChunkNameTable_ = New<TNameTable>();
        FromProto(&ChunkNameTable_, *nameTableExt);
    }
}

void TColumnarChunkMeta::InitBlockLastKeys(const TKeyColumns& keyColumns)
{
    int prefixLength = 0;
    while (prefixLength < keyColumns.size() && prefixLength < ChunkSchema_.GetKeyColumnCount()) {
        if (keyColumns[prefixLength] != ChunkSchema_.Columns()[prefixLength].Name()) {
            break;
        }
        ++prefixLength;
    }

    struct TBlockLastKeysBufferTag { };
    auto tempBuffer = New<TRowBuffer>(TBlockLastKeysBufferTag());

    std::vector<TKey> blockLastKeys;
    blockLastKeys.reserve(BlockMeta_->blocks_size());
    for (const auto& block : BlockMeta_->blocks()) {
        TKey key;
        if (ChunkSchema_.GetKeyColumnCount() > 0) {
            YCHECK(block.has_last_key());
            key = FromProto<TKey>(block.last_key(), tempBuffer);
        } else {
            key = tempBuffer->AllocateUnversioned(0);
        }
        auto wideKey = WidenKeyPrefix(key, prefixLength, keyColumns.size(), tempBuffer);
        blockLastKeys.push_back(wideKey);
    }

    std::tie(BlockLastKeys_, BlockLastKeysSize_) = CaptureRows<TBlockLastKeysBufferTag>(MakeRange(blockLastKeys));
}

i64 TColumnarChunkMeta::GetMemoryUsage() const
{
    return BlockLastKeysSize_ +
        sizeof(Misc_) +
        BlockMeta_->GetSize() +
        (ColumnMeta_ ? ColumnMeta_->GetSize() : 0);
    // TODO(psushin): account schema here, or make it ref-counted.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
