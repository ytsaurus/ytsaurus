#include "chunk_column_mapping.h"

#include <yt/yt/client/complex_types/check_type_compatibility.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateSchema(const TTableSchema& chunkSchema, const TTableSchema& readerSchema)
{
    auto throwIncompatibleKeyColumns = [&] () {
        THROW_ERROR_EXCEPTION(
            "Reader key column stable names %v are incompatible with chunk key column stable names %v",
            readerSchema.GetKeyColumnStableNames(),
            chunkSchema.GetKeyColumnStableNames());
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

            if (chunkColumn.StableName() != column.StableName() ||
                chunkColumn.GetWireType() != column.GetWireType() ||
                chunkColumn.SortOrder() != column.SortOrder())
            {
                throwIncompatibleKeyColumns();
            }
        } else {
            auto* chunkColumn = chunkSchema.FindColumnByStableName(column.StableName());
            if (chunkColumn) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible reader key columns: %Qv is a non-key column in chunk schema %v",
                    column.GetDiagnosticNameString(),
                    ConvertToYsonString(chunkSchema, NYson::EYsonFormat::Text).AsStringBuf());
            }
        }
    }

    for (int readerIndex = readerSchema.GetKeyColumnCount(); readerIndex < std::ssize(readerSchema.Columns()); ++readerIndex) {
        auto& readerColumn = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = chunkSchema.FindColumnByStableName(readerColumn.StableName());
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        auto compatibility = NComplexTypes::CheckTypeCompatibility(
            chunkColumn->LogicalType(),
            readerColumn.LogicalType());

        if (compatibility.first != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR_EXCEPTION(
                "Incompatible type %Qlv for column %Qv in chunk schema %v",
                *readerColumn.LogicalType(),
                readerColumn.GetDiagnosticNameString(),
                ConvertToYsonString(chunkSchema, NYson::EYsonFormat::Text).AsStringBuf())
                << compatibility.second;
        }
    }
}

DEFINE_REFCOUNTED_TYPE(TChunkColumnMapping)

TChunkColumnMapping::TChunkColumnMapping(const TTableSchemaPtr& tableSchema, const TTableSchemaPtr& chunkSchema)
    : TableKeyColumnCount_(tableSchema->GetKeyColumnCount())
    , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
{
    ValidateSchema(*chunkSchema, *tableSchema);

    ValueIdMapping_.resize(tableSchema->GetColumnCount() - TableKeyColumnCount_, -1);

    for (int index = TableKeyColumnCount_; index < tableSchema->GetColumnCount(); ++index) {
        auto& column = tableSchema->Columns()[index];

        auto* chunkColumn = chunkSchema->FindColumnByStableName(column.StableName());
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        auto chunkIndex = chunkSchema->GetColumnIndex(*chunkColumn);
        ValueIdMapping_[index - TableKeyColumnCount_] = chunkIndex;
    }
}

std::vector<TColumnIdMapping> TChunkColumnMapping::BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    std::vector<TColumnIdMapping> valueIdMapping;

    if (columnFilter.IsUniversal()) {
        valueIdMapping.reserve(std::ssize(ValueIdMapping_));

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
        auto indexes = MakeRange(columnFilter.GetIndexes());
        valueIdMapping.reserve(std::ssize(indexes));

        for (auto index : indexes) {
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
