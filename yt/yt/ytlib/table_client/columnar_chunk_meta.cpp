#include "columnar_chunk_meta.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/column_rename_descriptor.h>

namespace NYT::NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat::NProto;

using NYT::FromProto;

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
        YT_VERIFY(FromProto<ETableChunkFormat>(chunkMeta.format()) == ETableChunkFormat::SchemalessHorizontal);
        const auto keyColumns = NYT::FromProto<TKeyColumns>(*keyColumnsExt);
        schema = TTableSchema::FromKeyColumns(keyColumns);
    } else {
        // COMPAT(gritukan) This is used for very old chunks only.
        schema = New<TTableSchema>();
    }

    return schema;
}

////////////////////////////////////////////////////////////////////////////////

TColumnarChunkMeta::TColumnarChunkMeta(const TChunkMeta& chunkMeta)
{
    InitExtensions(chunkMeta);
}

void TColumnarChunkMeta::InitExtensions(const TChunkMeta& chunkMeta)
{
    ChunkType_ = CheckedEnumCast<EChunkType>(chunkMeta.type());
    ChunkFormat_ = CheckedEnumCast<ETableChunkFormat>(chunkMeta.format());

    Misc_ = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
    BlockMeta_ = New<TRefCountedBlockMeta>(GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions()));

    // This is for old horizontal versioned chunks, since TCachedVersionedChunkMeta use this call.
    if (auto columnMeta = FindProtoExtension<TColumnMetaExt>(chunkMeta.extensions())) {
        ColumnMeta_ = New<TRefCountedColumnMeta>(std::move(*columnMeta));
    }

    ChunkSchema_ = GetTableSchema(chunkMeta);

    if (auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions())) {
        ChunkNameTable_ = New<TNameTable>();
        FromProto(&ChunkNameTable_, *nameTableExt);
    }
}

void TColumnarChunkMeta::InitBlockLastKeys(const TKeyColumns& keyColumns)
{
    int prefixLength = 0;
    while (prefixLength < keyColumns.size() && prefixLength < ChunkSchema_->GetKeyColumnCount()) {
        if (keyColumns[prefixLength] != ChunkSchema_->Columns()[prefixLength].Name()) {
            break;
        }
        ++prefixLength;
    }

    struct TBlockLastKeysBufferTag { };
    auto tempBuffer = New<TRowBuffer>(TBlockLastKeysBufferTag());

    std::vector<TLegacyKey> legacyBlockLastKeys;
    legacyBlockLastKeys.reserve(BlockMeta_->blocks_size());
    for (const auto& block : BlockMeta_->blocks()) {
        TLegacyKey key;
        if (ChunkSchema_->GetKeyColumnCount() > 0) {
            YT_VERIFY(block.has_last_key());
            key = FromProto<TLegacyKey>(block.last_key(), tempBuffer);
        } else {
            key = tempBuffer->AllocateUnversioned(0);
        }
        auto wideKey = WidenKeyPrefix(key, prefixLength, keyColumns.size(), tempBuffer);
        legacyBlockLastKeys.push_back(wideKey);
    }

    std::tie(LegacyBlockLastKeys_, BlockLastKeysSize_) = CaptureRows<TBlockLastKeysBufferTag>(MakeRange(legacyBlockLastKeys));
    std::vector<TKey> blockLastKeys;
    blockLastKeys.reserve(LegacyBlockLastKeys_.size());
    for (const auto& lastKey : LegacyBlockLastKeys_) {
        blockLastKeys.push_back(TKey::FromRow(lastKey));
    }
    BlockLastKeys_ = MakeSharedRange(blockLastKeys, LegacyBlockLastKeys_.GetHolder());
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
                for (int id = 0; id < names.size(); ++id) {
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
    return
        BlockLastKeysSize_ +
        sizeof(TKey) * BlockLastKeys_.Size() +
        sizeof (Misc_) +
        BlockMeta_->GetSize() +
        (ColumnMeta_ ? ColumnMeta_->GetSize() : 0) +
        ChunkSchema_->GetMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
