#include "columnar_chunk_meta.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/column_rename_descriptor.h>

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
                auto newColumns = ChunkSchema_.Columns();
                for (auto& column : newColumns) {
                    auto it = nameMapping.find(column.Name());
                    if (it != nameMapping.end()) {
                        column.SetName(it->second);
                    }
                }
                ChunkSchema_ = TTableSchema(newColumns, ChunkSchema_.GetStrict(), ChunkSchema_.GetUniqueKeys());
                ValidateColumnUniqueness(ChunkSchema_);
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
    return BlockLastKeysSize_ +
        sizeof(Misc_) +
        BlockMeta_->GetSize() +
        (ColumnMeta_ ? ColumnMeta_->GetSize() : 0) +
        ChunkSchema_.GetMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
