#include "columnar_chunk_meta.h"

#include <yt/yt/client/table_client/key.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>

namespace NYT::NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat::NProto;

using NYT::FromProto;

struct TBlockLastKeysBufferTag { };

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr GetTableSchema(const NChunkClient::NProto::TChunkMeta& chunkMeta)
{
    TTableSchemaPtr schema;
    auto keyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
    auto tableSchemaExt = FindProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
    if (tableSchemaExt && keyColumnsExt) {
        FromProto(&schema, *tableSchemaExt, *keyColumnsExt);
    } else if (tableSchemaExt) {
        FromProto(&schema, *tableSchemaExt);
    } else if (keyColumnsExt) {
        // COMPAT(savrus) No table schema is allowed only for old chunks.
        YT_VERIFY(FromProto<EChunkFormat>(chunkMeta.format()) == EChunkFormat::TableUnversionedSchemalessHorizontal);
        const auto keyColumns = NYT::FromProto<TKeyColumns>(*keyColumnsExt);
        schema = TTableSchema::FromKeyColumns(keyColumns);
    } else {
        // COMPAT(gritukan) This is used for very old chunks only.
        schema = New<TTableSchema>();
    }

    return schema;
}

int GetCommonKeyPrefix(const TKeyColumns& lhs, const TKeyColumns& rhs)
{
    int prefixLength = 0;
    while (prefixLength < std::ssize(lhs) && prefixLength < std::ssize(rhs)) {
        if (lhs[prefixLength] != rhs[prefixLength]) {
            break;
        }
        ++prefixLength;
    }

    return prefixLength;
}

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkMeta::TColumnarChunkMeta(const TChunkMeta& chunkMeta)
{
    ChunkType_ = CheckedEnumCast<EChunkType>(chunkMeta.type());
    ChunkFormat_ = CheckedEnumCast<EChunkFormat>(chunkMeta.format());

    Misc_ = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
    DataBlockMeta_ = New<TRefCountedDataBlockMeta>(GetProtoExtension<TDataBlockMetaExt>(chunkMeta.extensions()));
    if (HasProtoExtension<TSystemBlockMetaExt>(chunkMeta.extensions())) {
        SystemBlockMeta_ = New<TRefCountedSystemBlockMeta>(GetProtoExtension<TSystemBlockMetaExt>(chunkMeta.extensions()));
    }

    // This is for old horizontal versioned chunks, since TCachedVersionedChunkMeta use this call.
    if (auto columnMeta = FindProtoExtension<TColumnMetaExt>(chunkMeta.extensions())) {
        ColumnMeta_ = New<TRefCountedColumnMeta>(std::move(*columnMeta));
    }

    ChunkSchema_ = GetTableSchema(chunkMeta);

    if (auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions())) {
        ChunkNameTable_ = New<TNameTable>();
        FromProto(&ChunkNameTable_, *nameTableExt);
    }

    auto buffer = New<TRowBuffer>(TBlockLastKeysBufferTag());

    std::vector<TUnversionedRow> blockLastKeys;
    blockLastKeys.reserve(DataBlockMeta_->data_blocks_size());
    for (const auto& block : DataBlockMeta_->data_blocks()) {
        TUnversionedRow key;

        // Block last keys are not supported in partition chunks.
        if (block.has_last_key()) {
            if (ChunkSchema_->GetKeyColumnCount() > 0) {
                YT_VERIFY(block.has_last_key());
                key = FromProto<TUnversionedRow>(block.last_key(), buffer);
            } else {
                key = buffer->AllocateUnversioned(0);
            }
        }
        blockLastKeys.push_back(key);
    }

    BlockLastKeysSize_ = buffer->GetCapacity();

    BlockLastKeys_ = MakeSharedRange(
        blockLastKeys,
        std::move(buffer));

    if (auto optionalHunkChunkRefsExt = FindProtoExtension<THunkChunkRefsExt>(chunkMeta.extensions())) {
        HunkChunkRefsExt_ = std::move(*optionalHunkChunkRefsExt);
    }

    if (auto optionalHunkChunkMetasExt = FindProtoExtension<THunkChunkMetasExt>(chunkMeta.extensions())) {
        HunkChunkMetasExt_ = std::move(*optionalHunkChunkMetasExt);
    }
}

i64 TColumnarChunkMeta::GetMemoryUsage() const
{
    // We use SpaceUsed() protobuf method to estimate memory consumption.
    // That method does not account for internal allocator fragmentation and
    // misses some internal protobuf data structures.
    const auto metaMemoryFactor = 2;

    return
        BlockLastKeysSize_ +
        sizeof (TKey) * BlockLastKeys_.Size() +
        Misc_.SpaceUsedLong() +
        DataBlockMeta_->GetSize() * metaMemoryFactor +
        (SystemBlockMeta_ ? SystemBlockMeta_->GetSize() * metaMemoryFactor : 0) +
        (ColumnMeta_ ? ColumnMeta_->GetSize() * metaMemoryFactor : 0) +
        ChunkSchema_->GetMemoryUsage();
}

void TColumnarChunkMeta::ClearColumnMeta()
{
    ColumnMeta_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
