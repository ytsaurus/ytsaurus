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
        YT_VERIFY(CheckedEnumCast<EChunkFormat>(chunkMeta.format()) == EChunkFormat::TableSchemalessHorizontal);
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
}

void TColumnarChunkMeta::RenameColumns(const TColumnRenameDescriptors& renameDescriptors)
{
    if (!renameDescriptors.empty()) {
        THashMap<TString, TString> nameMapping;
        for (const auto& descriptor : renameDescriptors) {
            nameMapping[descriptor.OriginalName] = descriptor.NewName;
        }
        try {
            // ChunkSchema
            {
                auto newColumns = ChunkSchema_->Columns();
                for (auto& column : newColumns) {
                    auto it = nameMapping.find(column.Name());
                    if (it != nameMapping.end()) {
                        column.SetName(it->second);
                    }
                }
                ChunkSchema_ = New<TTableSchema>(newColumns, ChunkSchema_->GetStrict(), ChunkSchema_->GetUniqueKeys());
                ValidateColumnUniqueness(*ChunkSchema_);
            }
            // ChunkNameTable
            if (ChunkNameTable_) {
                std::vector<TString> names;
                names.resize(ChunkNameTable_->GetSize());
                for (int id = 0; id < std::ssize(names); ++id) {
                    names[id] = ChunkNameTable_->GetName(id);
                    auto it = nameMapping.find(names[id]);
                    if (it != nameMapping.end()) {
                        names[id] = it->second;
                    }
                }
                ChunkNameTable_ = New<TNameTable>();
                for (auto& name : names) {
                    ChunkNameTable_->RegisterNameOrThrow(std::move(name));
                }
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(EErrorCode::InvalidColumnRenaming, "Error renaming columns in chunk meta")
                << TErrorAttribute("column_rename_descriptors", renameDescriptors)
                << ex;
        }
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
        (ColumnMeta_ ? ColumnMeta_->GetSize() * metaMemoryFactor : 0) +
        ChunkSchema_->GetMemoryUsage();
}

void TColumnarChunkMeta::ClearColumnMeta()
{
    ColumnMeta_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
