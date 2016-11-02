#include "columnar_chunk_meta.h"

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
    BlockMeta_ = GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions());

    // This is for old horizontal versioned chunks, since TCachedVersionedChunkMeta use this call.
    auto columnMeta = FindProtoExtension<TColumnMetaExt>(chunkMeta.extensions());
    if (columnMeta) {
        ColumnMeta_.Swap(&*columnMeta);
    }

    auto maybeKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
    auto tableSchemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
    if (maybeKeyColumnsExt) {
        FromProto(&ChunkSchema_, tableSchemaExt, *maybeKeyColumnsExt);
    } else {
        FromProto(&ChunkSchema_, tableSchemaExt);
    }
}

void TColumnarChunkMeta::InitBlockLastKeys(const TKeyColumns& keyColumns)
{
    int prefixLength = 0;
    while (prefixLength < keyColumns.size() && prefixLength < ChunkSchema_.GetKeyColumnCount()) {
        if (keyColumns[prefixLength] != ChunkSchema_.Columns()[prefixLength].Name) {
            break;
        }
        ++prefixLength;
    }

    BlockLastKeys_.reserve(BlockMeta_.blocks_size());
    for (const auto& block : BlockMeta_.blocks()) {
        YCHECK(block.has_last_key());
        auto key = FromProto<TOwningKey>(block.last_key());
        BlockLastKeys_.push_back(WidenKeyPrefix(key, prefixLength, keyColumns.size()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
