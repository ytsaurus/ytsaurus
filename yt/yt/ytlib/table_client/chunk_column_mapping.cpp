#include "chunk_column_mapping.h"

#include <yt/yt/client/complex_types/check_type_compatibility.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateSchema(const TTableSchema& chunkSchema, const TTableSchema& readerSchema)
{
    auto throwIncompatibleKeyColumns = [&] {
        THROW_ERROR_EXCEPTION(
            "Reader key column stable names %v are incompatible with chunk key column stable names %v",
            readerSchema.GetKeyColumnStableNames(),
            chunkSchema.GetKeyColumnStableNames());
    };

    if (readerSchema.GetKeyColumnCount() < chunkSchema.GetKeyColumnCount()) {
        throwIncompatibleKeyColumns();
    }

    for (auto readerIndex : std::views::iota(0, readerSchema.GetKeyColumnCount())) {
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
        } else if (chunkSchema.FindColumnByStableName(column.StableName())) {
            THROW_ERROR_EXCEPTION(
                "Incompatible reader key columns: %Qv is a non-key column in chunk schema %v",
                column.GetDiagnosticNameString(),
                ConvertToYsonString(chunkSchema, NYson::EYsonFormat::Text).AsStringBuf());
        }
    }

    for (const auto& readerColumn : readerSchema.Columns() | std::views::drop(readerSchema.GetKeyColumnCount())) {
        auto* chunkColumn = chunkSchema.FindColumnByStableName(readerColumn.StableName());
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        static constexpr NComplexTypes::TTypeCompatibilityOptions TypeCompatibilityOptions{
            .AllowStructFieldRenaming = true,
            .AllowStructFieldRemoval = true,
            .IgnoreUnknownRemovedFieldNames = true,
        };
        auto compatibility = NComplexTypes::CheckTypeCompatibility(
            chunkColumn->LogicalType(),
            readerColumn.LogicalType(),
            TypeCompatibilityOptions);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace

TChunkColumnMapping::TChunkColumnMapping(
    const TTableSchemaPtr& tableSchema,
    const TTableSchemaPtr& chunkSchema)
    : TableKeyColumnCount_(tableSchema->GetKeyColumnCount())
    , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
    , TableValueIndexToChunkIndex_(std::invoke([&] {
        ValidateSchema(*chunkSchema, *tableSchema);

        std::vector<int> result(tableSchema->GetColumnCount() - TableKeyColumnCount_, -1);
        for (auto index : std::views::iota(TableKeyColumnCount_, tableSchema->GetColumnCount())) {
            const auto& column = tableSchema->Columns()[index];

            // NB: Column might be absent in chunk.
            if (auto* chunkColumn = chunkSchema->FindColumnByStableName(column.StableName())) {
                result[index - TableKeyColumnCount_] = chunkSchema->GetColumnIndex(*chunkColumn);
            }
        }
        return result;
    }))
{ }

std::vector<TColumnIdMapping> TChunkColumnMapping::BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    if (columnFilter.IsUniversal()) {
        auto resultRange = std::views::iota(0, std::ssize(TableValueIndexToChunkIndex_))
            | std::views::filter([&] (auto tableValueIndex) {
                return TableValueIndexToChunkIndex_[tableValueIndex] >= 0;
            })
            | std::views::transform([&] (auto tableValueIndex) {
                return TColumnIdMapping{
                    .ChunkSchemaIndex = TableValueIndexToChunkIndex_[tableValueIndex],
                    .ReaderSchemaIndex = TableKeyColumnCount_ + tableValueIndex,
                };
            })
            | std::views::common;

        // TODO(s-berdnikov): Use std::ranges::to once C++23 is avaliable.
        return {resultRange.begin(), resultRange.end()};
    }

    auto resultRange = columnFilter.GetIndexes()
        | std::views::filter([&] (auto index) {
            return index >= TableKeyColumnCount_ &&
                TableValueIndexToChunkIndex_[index - TableKeyColumnCount_] >= 0;
        })
        | std::views::transform([&] (auto index) {
            return TColumnIdMapping{
                .ChunkSchemaIndex = TableValueIndexToChunkIndex_[index - TableKeyColumnCount_],
                .ReaderSchemaIndex = index,
            };
        })
        | std::views::common;

    // TODO(s-berdnikov): Use std::ranges::to once C++23 is avaliable.
    return {resultRange.begin(), resultRange.end()};
}

std::vector<int> TChunkColumnMapping::BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter) const
{
    std::vector<int> result(ChunkColumnCount_, -1);
    std::iota(result.begin(), result.begin() + ChunkKeyColumnCount_, 0);

    if (columnFilter.IsUniversal()) {
        for (auto tableValueIndex : std::views::iota(0, std::ssize(TableValueIndexToChunkIndex_))) {
            auto chunkIndex = TableValueIndexToChunkIndex_[tableValueIndex];
            if (chunkIndex < 0) {
                continue;
            }

            YT_VERIFY(chunkIndex < std::ssize(result));
            YT_VERIFY(chunkIndex >= ChunkKeyColumnCount_);
            result[chunkIndex] = TableKeyColumnCount_ + tableValueIndex;
        }
        return result;
    }

    for (auto index : columnFilter.GetIndexes()) {
        if (index < TableKeyColumnCount_) {
            continue;
        }

        auto chunkIndex = TableValueIndexToChunkIndex_[index - TableKeyColumnCount_];
        if (chunkIndex < 0) {
            continue;
        }

        YT_VERIFY(chunkIndex < std::ssize(result));
        YT_VERIFY(chunkIndex >= ChunkKeyColumnCount_);
        result[chunkIndex] = index;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
