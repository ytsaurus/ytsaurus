#include "chunk_column_mapping.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping);

TChunkColumnMapping::TChunkColumnMapping(const TTableSchemaPtr& tableSchema, const TTableSchemaPtr& chunkSchema)
    : SchemaKeyColumnCount_(tableSchema->GetKeyColumnCount())
    , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
{
    Mapping_.resize(tableSchema->GetColumnCount() - SchemaKeyColumnCount_, -1);

    THashMap<TStringBuf, int> chunkValueColumnNames;
    for (int index = chunkSchema->GetKeyColumnCount(); index < chunkSchema->GetColumnCount(); ++index) {
        const auto& column = chunkSchema->Columns()[index];
        chunkValueColumnNames.emplace(column.Name(), index);
    }

    for (int index = SchemaKeyColumnCount_; index < tableSchema->GetColumnCount(); ++index) {
        auto& column = tableSchema->Columns()[index];

        auto it = chunkValueColumnNames.find(column.Name());
        if (it == chunkValueColumnNames.end()) {
            // This is a valid case, simply skip the column.
            continue;
        }

        auto chunkIndex = it->second;
        Mapping_[index - SchemaKeyColumnCount_] = chunkIndex;
    }
}

std::vector<TColumnIdMapping> TChunkColumnMapping::BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    std::vector<TColumnIdMapping> valueIdMapping;

    if (columnFilter.IsUniversal()) {
        for (int schemaValueIndex = 0; schemaValueIndex < std::ssize(Mapping_); ++schemaValueIndex) {
            auto chunkIndex = Mapping_[schemaValueIndex];
            if (chunkIndex == -1) {
                continue;
            }

            TColumnIdMapping mapping;
            mapping.ChunkSchemaIndex = chunkIndex;
            mapping.ReaderSchemaIndex = schemaValueIndex + SchemaKeyColumnCount_;
            valueIdMapping.push_back(mapping);
        }
    } else {
        for (auto index : columnFilter.GetIndexes()) {
            if (index < SchemaKeyColumnCount_) {
                continue;
            }

            auto chunkIndex = Mapping_[index - SchemaKeyColumnCount_];
            if (chunkIndex == -1) {
                continue;
            }

            TColumnIdMapping mapping;
            mapping.ChunkSchemaIndex = chunkIndex;
            mapping.ReaderSchemaIndex = index;
            valueIdMapping.push_back(mapping);
        }
    }

    return valueIdMapping;
}

std::vector<TColumnIdMapping> TChunkColumnMapping::BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    std::vector<TColumnIdMapping> idMapping(
        ChunkColumnCount_,
        TColumnIdMapping{-1,-1});

    int chunkKeyColumnCount = ChunkKeyColumnCount_;
    for (int index = 0; index < chunkKeyColumnCount; ++index) {
        idMapping[index].ReaderSchemaIndex = index;
    }

    if (columnFilter.IsUniversal()) {
        for (int schemaValueIndex = 0; schemaValueIndex < std::ssize(Mapping_); ++schemaValueIndex) {
            auto chunkIndex = Mapping_[schemaValueIndex];
            if (chunkIndex == -1) {
                continue;
            }

            YT_VERIFY(chunkIndex < std::ssize(idMapping));
            YT_VERIFY(chunkIndex >= chunkKeyColumnCount);
            idMapping[chunkIndex].ReaderSchemaIndex = schemaValueIndex + SchemaKeyColumnCount_;
        }
    } else {
        for (auto index : columnFilter.GetIndexes()) {
            if (index < SchemaKeyColumnCount_) {
                continue;
            }

            auto chunkIndex = Mapping_[index - SchemaKeyColumnCount_];
            if (chunkIndex == -1) {
                continue;
            }

            YT_VERIFY(chunkIndex < std::ssize(idMapping));
            YT_VERIFY(chunkIndex >= chunkKeyColumnCount);
            idMapping[chunkIndex].ReaderSchemaIndex = index;
        }
    }

    return idMapping;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
