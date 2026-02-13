#include "yt_to_ch_block_converter.h"

#include "yt_to_ch_column_converter.h"

#include <library/cpp/iterator/zip.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/attributes.h>

#include <library/cpp/iterator/enumerate.h>

#include <Core/Block.h>
#include <Columns/IColumn.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TYTToCHBlockConverter::TImpl
{
public:
    TImpl(
        const std::vector<TColumnSchema>& readColumnSchemas,
        const std::vector<NYTree::IAttributeDictionaryPtr>& columnAttributes,
        const TNameTablePtr& nameTable,
        const TCompositeSettingsPtr& compositeSettings,
        bool optimizeDistinctRead)
        : OptimizeDistinctRead_(optimizeDistinctRead)
    {
        int columnCount = readColumnSchemas.size();

        ColumnConverters_.reserve(columnCount);

        DB::ColumnsWithTypeAndName headerColumnTypeAndNames;
        headerColumnTypeAndNames.reserve(columnCount);

        YT_VERIFY(columnAttributes.empty() || readColumnSchemas.size() == columnAttributes.size());
        for (int i = 0; i < std::ssize(readColumnSchemas); ++i) {
            const auto& converter = ColumnConverters_.emplace_back(
                TComplexTypeFieldDescriptor(readColumnSchemas[i]),
                compositeSettings,
                columnAttributes.empty() ? false : columnAttributes[i]->Get<bool>(LowCardinalityAttribute, false));
                headerColumnTypeAndNames.emplace_back(converter.GetDataType(), readColumnSchemas[i].Name());
        }

        HeaderBlock_ = DB::Block(std::move(headerColumnTypeAndNames));

        int maxId = 0;
        std::vector<int> columnIndexToId(columnCount);
        for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
            int id = nameTable->GetIdOrRegisterName(readColumnSchemas[columnIndex].Name());
            columnIndexToId[columnIndex] = id;
            maxId = std::max(maxId, id);
        }

        IdToColumnIndex_.resize(maxId + 1, -1);
        for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
            IdToColumnIndex_[columnIndexToId[columnIndex]] = columnIndex;
        }
    }

    const DB::Block& GetHeaderBlock() const
    {
        return HeaderBlock_;
    }

    DB::Block Convert(const NTableClient::IUnversionedRowBatchPtr& batch, TRange<DB::UInt8> filterHint)
    {

        int columnCount = HeaderBlock_.columns();
        // NB(max42): CHYT-256.
        // If chunk schema contains not all of the requested columns (which may happen
        // when a non-required column was introduced after chunk creation), we are not
        // going to receive some of the unversioned values with nulls. We still need
        // to provide them to CH, though, so we keep track of present columns for each
        // row we get and add nulls for all unpresent columns.
        std::vector<bool> presentColumnMask(columnCount);

        for (auto& columnConverter : ColumnConverters_) {
            columnConverter.InitColumn();
        }

        if (auto columnarBatch = batch->TryAsColumnar()) {
            auto batchColumns = columnarBatch->MaterializeColumns();
            for (const auto* ytColumn : batchColumns) {
                auto id = ytColumn->Id;
                auto columnIndex = (id < std::ssize(IdToColumnIndex_)) ? IdToColumnIndex_[id] : -1;
                bool needConsumeNull = false;
                if (OptimizeDistinctRead_) {
                    if (ytColumn->Rle) {
                        ytColumn = ytColumn->Rle->ValueColumn;
                    }
                    if (ytColumn->Dictionary) {
                        if (ytColumn->Dictionary->ZeroMeansNull) {
                            auto dictionaryIndexes = ytColumn->GetTypedValues<ui32>();
                            for (i64 index = ytColumn->StartIndex; index < ytColumn->StartIndex + ytColumn->ValueCount; ++index) {
                                if (dictionaryIndexes[index] == 0) {
                                    needConsumeNull = true;
                                    break;
                                }
                            }
                        }
                        ytColumn = ytColumn->Dictionary->ValueColumn;
                    }
                }
                if (columnIndex != -1) {
                    YT_VERIFY(columnIndex < columnCount);
                    YT_VERIFY(!presentColumnMask[columnIndex]);
                    ColumnConverters_[columnIndex].ConsumeYtColumn(*ytColumn, filterHint);
                    if (needConsumeNull) {
                        ColumnConverters_[columnIndex].ConsumeNulls(1);
                    }
                    presentColumnMask[columnIndex] = true;
                }
            }
            for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
                if (!presentColumnMask[columnIndex]) {
                    ColumnConverters_[columnIndex].ConsumeNulls(batch->GetRowCount());
                }
            }
        } else {
            auto rowBatch = batch->MaterializeRows();
            // We transpose rows by writing down contiguous range of values for each column.
            // This is done to reduce the number of converter virtual calls.
            std::vector<std::vector<TUnversionedValue>> columnIndexToUnversionedValues(columnCount);
            for (auto& unversionedValues : columnIndexToUnversionedValues) {
                unversionedValues.reserve(rowBatch.size());
            }

            auto nullValue = MakeUnversionedNullValue();

            int rowIndex = 0;
            for (auto row : rowBatch) {
                presentColumnMask.assign(columnCount, false);

                if (!filterHint || filterHint[rowIndex++]) {
                    for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
                        auto value = row[index];
                        auto id = value.Id;
                        int columnIndex = (id < IdToColumnIndex_.size()) ? IdToColumnIndex_[id] : -1;
                        if (columnIndex != -1) {
                            YT_VERIFY(columnIndex < columnCount);
                            YT_VERIFY(!presentColumnMask[columnIndex]);
                            columnIndexToUnversionedValues[columnIndex].emplace_back(value);
                            presentColumnMask[columnIndex] = true;
                        }
                    }
                }

                for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
                    if (!presentColumnMask[columnIndex]) {
                        // NB: converter does not care about value ids.
                        columnIndexToUnversionedValues[columnIndex].emplace_back(nullValue);
                    }
                }
            }

            for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
                const auto& unversionedValues = columnIndexToUnversionedValues[columnIndex];
                YT_VERIFY(unversionedValues.size() == rowBatch.size());
                auto& converter = ColumnConverters_[columnIndex];
                converter.ConsumeUnversionedValues(unversionedValues);
            }
        }

        auto block = HeaderBlock_.cloneEmpty();
        for (const auto& [columnIndex, converter] : Enumerate(ColumnConverters_)) {
            auto column = converter.FlushColumn();
            YT_VERIFY(OptimizeDistinctRead_ || std::ssize(*column) == batch->GetRowCount());
            block.getByPosition(columnIndex).column = std::move(column);
        }
        YT_VERIFY(filterHint.empty() || block.rows() == 0 || filterHint.size() == block.rows());

        return block;
    }

private:
    DB::Block HeaderBlock_;
    std::vector<TYTToCHColumnConverter> ColumnConverters_;
    std::vector<int> IdToColumnIndex_;
    bool OptimizeDistinctRead_;
};

////////////////////////////////////////////////////////////////////////////////

TYTToCHBlockConverter::TYTToCHBlockConverter(
    const std::vector<TColumnSchema>& readColumnSchemas,
    const std::vector<NYTree::IAttributeDictionaryPtr>& columnAttributes,
    const TNameTablePtr& nameTable,
    const TCompositeSettingsPtr& compositeSettings,
    bool optimizeDistinctRead)
    : Impl_(std::make_unique<TImpl>(readColumnSchemas, columnAttributes, nameTable, compositeSettings, optimizeDistinctRead))
{ }

TYTToCHBlockConverter::TYTToCHBlockConverter(TYTToCHBlockConverter&& other) = default;

TYTToCHBlockConverter::~TYTToCHBlockConverter() = default;

const DB::Block& TYTToCHBlockConverter::GetHeaderBlock() const
{
    return Impl_->GetHeaderBlock();
}

DB::Block TYTToCHBlockConverter::Convert(const NTableClient::IUnversionedRowBatchPtr& batch, TRange<DB::UInt8> filterHint)
{
    return Impl_->Convert(batch, filterHint);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
