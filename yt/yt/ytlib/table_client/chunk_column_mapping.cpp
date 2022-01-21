#include "chunk_column_mapping.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping);

TChunkColumnMapping::TChunkColumnMapping(const TTableSchemaPtr& tableSchema, const TTableSchemaPtr& chunkSchema)
    : TableKeyColumnCount_(tableSchema->GetKeyColumnCount())
    , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
{
    ValueIdMapping_.resize(tableSchema->GetColumnCount() - TableKeyColumnCount_, -1);

    THashMap<TStringBuf, int> chunkValueColumnNames;
    for (int index = chunkSchema->GetKeyColumnCount(); index < chunkSchema->GetColumnCount(); ++index) {
        const auto& column = chunkSchema->Columns()[index];
        chunkValueColumnNames.emplace(column.Name(), index);
    }

    for (int index = TableKeyColumnCount_; index < tableSchema->GetColumnCount(); ++index) {
        auto& column = tableSchema->Columns()[index];

        auto it = chunkValueColumnNames.find(column.Name());
        if (it == chunkValueColumnNames.end()) {
            // This is a valid case, simply skip the column.
            continue;
        }

        auto chunkIndex = it->second;
        ValueIdMapping_[index - TableKeyColumnCount_] = chunkIndex;
    }
}

std::vector<TColumnIdMapping> TChunkColumnMapping::BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    std::vector<TColumnIdMapping> valueIdMapping;

    if (columnFilter.IsUniversal()) {
        for (int schemaValueIndex = 0; schemaValueIndex < std::ssize(ValueIdMapping_); ++schemaValueIndex) {
            auto chunkIndex = ValueIdMapping_[schemaValueIndex];
            if (chunkIndex == -1) {
                continue;
            }

            TColumnIdMapping mapping;
            mapping.ChunkSchemaIndex = chunkIndex;
            mapping.ReaderSchemaIndex = schemaValueIndex + TableKeyColumnCount_;
            valueIdMapping.push_back(mapping);
        }
    } else {
        for (auto index : columnFilter.GetIndexes()) {
            if (index < TableKeyColumnCount_) {
                continue;
            }

            auto chunkIndex = ValueIdMapping_[index - TableKeyColumnCount_];
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

std::vector<int> TChunkColumnMapping::BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    std::vector<int> chunkToReaderIdMapping(ChunkColumnCount_, -1);

    int chunkKeyColumnCount = ChunkKeyColumnCount_;
    for (int index = 0; index < chunkKeyColumnCount; ++index) {
        chunkToReaderIdMapping[index] = index;
    }

    if (columnFilter.IsUniversal()) {
        for (int schemaValueIndex = 0; schemaValueIndex < std::ssize(ValueIdMapping_); ++schemaValueIndex) {
            auto chunkIndex = ValueIdMapping_[schemaValueIndex];
            if (chunkIndex == -1) {
                continue;
            }

            YT_VERIFY(chunkIndex < std::ssize(chunkToReaderIdMapping));
            YT_VERIFY(chunkIndex >= chunkKeyColumnCount);
            chunkToReaderIdMapping[chunkIndex] = schemaValueIndex + TableKeyColumnCount_;
        }
    } else {
        for (auto index : columnFilter.GetIndexes()) {
            if (index < TableKeyColumnCount_) {
                continue;
            }

            auto chunkIndex = ValueIdMapping_[index - TableKeyColumnCount_];
            if (chunkIndex == -1) {
                continue;
            }

            YT_VERIFY(chunkIndex < std::ssize(chunkToReaderIdMapping));
            YT_VERIFY(chunkIndex >= chunkKeyColumnCount);
            chunkToReaderIdMapping[chunkIndex] = index;
        }
    }

    return chunkToReaderIdMapping;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
