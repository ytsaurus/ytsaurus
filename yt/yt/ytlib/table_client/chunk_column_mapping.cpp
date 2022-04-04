#include "chunk_column_mapping.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateSchema(const TTableSchema& chunkSchema, const TTableSchema& readerSchema)
{
    auto throwIncompatibleKeyColumns = [&] () {
        THROW_ERROR_EXCEPTION(
            "Reader key columns %v are incompatible with chunk key columns %v",
            readerSchema.GetKeyColumns(),
            chunkSchema.GetKeyColumns());
    };

    if (readerSchema.GetKeyColumnCount() < chunkSchema.GetKeyColumnCount()) {
        throwIncompatibleKeyColumns();
    }

    for (int readerIndex = 0; readerIndex < readerSchema.GetKeyColumnCount(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        YT_VERIFY (column.SortOrder());

        if (readerIndex < chunkSchema.GetKeyColumnCount()) {
            const auto& chunkColumn = chunkSchema.Columns()[readerIndex];
            YT_VERIFY(chunkColumn.SortOrder());

            if (chunkColumn.Name() != column.Name() ||
                chunkColumn.GetWireType() != column.GetWireType() ||
                chunkColumn.SortOrder() != column.SortOrder())
            {
                throwIncompatibleKeyColumns();
            }
        } else {
            auto* chunkColumn = chunkSchema.FindColumn(column.Name());
            if (chunkColumn) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible reader key columns: %Qv is a non-key column in chunk schema %v",
                    column.Name(),
                    ConvertToYsonString(chunkSchema, NYson::EYsonFormat::Text).AsStringBuf());
            }
        }
    }

    for (int readerIndex = readerSchema.GetKeyColumnCount(); readerIndex < std::ssize(readerSchema.Columns()); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = chunkSchema.FindColumn(column.Name());
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        if (chunkColumn->GetWireType() != column.GetWireType()) {
            THROW_ERROR_EXCEPTION(
                "Incompatible type %Qlv for column %Qv in chunk schema %v",
                column.GetWireType(),
                column.Name(),
                ConvertToYsonString(chunkSchema, NYson::EYsonFormat::Text).AsStringBuf());
        }
    }
}

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping);

TChunkColumnMapping::TChunkColumnMapping(const TTableSchemaPtr& tableSchema, const TTableSchemaPtr& chunkSchema)
    : TableKeyColumnCount_(tableSchema->GetKeyColumnCount())
    , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
{
    ValidateSchema(*chunkSchema, *tableSchema);

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
