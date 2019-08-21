#include "blob_reader.h"

#include "name_table.h"
#include "schema.h"

#include <yt/core/concurrency/async_stream.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const TString TBlobTableSchema::PartIndexColumn = "part_index";
const TString TBlobTableSchema::DataColumn = "data";

TTableSchema TBlobTableSchema::ToTableSchema() const
{
    auto columns = BlobIdColumns;
    for (auto& idColumn : columns) {
        idColumn.SetSortOrder(ESortOrder::Ascending);
    }
    columns.emplace_back(PartIndexColumn, EValueType::Int64);
    columns.back().SetSortOrder(ESortOrder::Ascending);
    columns.emplace_back(DataColumn, EValueType::String);
    return TTableSchema(
        std::move(columns),
        true, // strict
        true); // uniqueKeys
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EColumnType,
    ((PartIndex) (0))
    ((Data)      (1))
);

class TBlobTableReader
    : public IAsyncZeroCopyInputStream
{
public:
    TBlobTableReader(
        ITableReaderPtr reader,
        const std::optional<TString>& partIndexColumnName,
        const std::optional<TString>& dataColumnName,
        i64 startPartIndex,
        const std::optional<i64>& offset,
        const std::optional<i64>& partSize)
        : Reader_(std::move(reader))
        , PartIndexColumnName_(partIndexColumnName ? *partIndexColumnName : TBlobTableSchema::PartIndexColumn)
        , DataColumnName_(dataColumnName ? *dataColumnName : TBlobTableSchema::DataColumn)
        , Offset_(offset.value_or(0))
        , PartSize_(partSize)
        , PreviousPartSize_(partSize)
        , NextPartIndex_(startPartIndex)
    {
        Rows_.reserve(1);
        ColumnIndex_[EColumnType::PartIndex] = Reader_->GetNameTable()->GetIdOrRegisterName(PartIndexColumnName_);
        ColumnIndex_[EColumnType::Data] = Reader_->GetNameTable()->GetIdOrRegisterName(DataColumnName_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        if (Index_ == Rows_.size()) {
            Index_ = 0;
            bool result = Reader_->Read(&Rows_);
            if (result && Rows_.empty()) {
                return Reader_->GetReadyEvent().Apply(BIND([this, this_ = MakeStrong(this)] () {
                    Reader_->Read(&Rows_);
                    return ProcessRow();
                }));
            }
        }
        return MakeFuture(ProcessRow());
    }

private:
    const ITableReaderPtr Reader_;
    const TString PartIndexColumnName_;
    const TString DataColumnName_;

    i64 Offset_;
    std::optional<i64> PartSize_;
    std::optional<i64> PreviousPartSize_;

    std::vector<TUnversionedRow> Rows_;
    i64 Index_ = 0;
    i64 NextPartIndex_;

    TEnumIndexedVector<EColumnType, std::optional<size_t>> ColumnIndex_;

    TSharedRef ProcessRow()
    {
        if (Rows_.empty()) {
            return TSharedRef();
        }

        auto row = Rows_[Index_++];
        auto value = GetDataAndValidateRow(row);

        auto holder = MakeIntrinsicHolder(Reader_);
        auto result = TSharedRef(value.Data.String, value.Length, std::move(holder));
        if (Offset_ > 0) {
            if (Offset_ > result.Size()) {
                THROW_ERROR_EXCEPTION("Offset is out of bounds")
                    << TErrorAttribute("offset", Offset_)
                    << TErrorAttribute("part_size", result.Size())
                    << TErrorAttribute("part_index", NextPartIndex_ - 1);
            }
            result = result.Slice(result.Begin() + Offset_, result.End());
            Offset_ = 0;
        }
        return result;
    }

    TUnversionedValue GetAndValidateValue(
        TUnversionedRow row,
        const TString& name,
        EColumnType columnType,
        EValueType expectedType)
    {
        auto columnIndex = ColumnIndex_[columnType];
        if (!columnIndex) {
            THROW_ERROR_EXCEPTION("Column %Qv not found", name);
        }

        TUnversionedValue columnValue;
        bool found = false;
        // NB: It is impossible to determine column index fast in schemaless reader.
        for (const auto& value : row) {
            if (value.Id == *columnIndex) {
                columnValue = value;
                found = true;
                break;
            }
        }

        if (!found) {
            THROW_ERROR_EXCEPTION("Column %Qv not found", name);
        }

        if (columnValue.Type != expectedType) {
            THROW_ERROR_EXCEPTION("Column %Qv must be of type %Qlv but has type %Qlv",
                name,
                expectedType,
                columnValue.Type);
        }

        return columnValue;
    }

    TUnversionedValue GetDataAndValidateRow(TUnversionedRow row)
    {
        auto partIndexValue = GetAndValidateValue(row, PartIndexColumnName_, EColumnType::PartIndex, EValueType::Int64);
        auto partIndex = partIndexValue.Data.Int64;

        if (partIndex != NextPartIndex_) {
            THROW_ERROR_EXCEPTION("Values of column %Qv must be consecutive but values %v and %v violate this property",
                PartIndexColumnName_,
                NextPartIndex_,
                partIndex);
        }

        NextPartIndex_ = partIndex + 1;

        auto value = GetAndValidateValue(row, DataColumnName_, EColumnType::Data, EValueType::String);

        auto isPreviousPartWrong = PartSize_ && *PreviousPartSize_ != *PartSize_;
        auto isCurrentPartWrong = PartSize_ && value.Length > *PartSize_;
        if (isPreviousPartWrong || isCurrentPartWrong) {
            i64 actualSize;
            i64 wrongPartIndex;
            if (isPreviousPartWrong) {
                actualSize = *PreviousPartSize_;
                wrongPartIndex = partIndex - 1;
            } else {
                actualSize = value.Length;
                wrongPartIndex = partIndex;
            }

            THROW_ERROR_EXCEPTION("Inconsistent part size")
                << TErrorAttribute("expected_size", *PartSize_)
                << TErrorAttribute("actual_size", actualSize)
                << TErrorAttribute("part_index", wrongPartIndex);
        }
        PreviousPartSize_ = value.Length;
        return value;
    }
};

IAsyncZeroCopyInputStreamPtr CreateBlobTableReader(
    ITableReaderPtr reader,
    const std::optional<TString>& partIndexColumnName,
    const std::optional<TString>& dataColumnName,
    i64 startPartIndex,
    const std::optional<i64>& offset,
    const std::optional<i64>& partSize)
{
    return New<TBlobTableReader>(
        std::move(reader),
        partIndexColumnName,
        dataColumnName,
        startPartIndex,
        offset,
        partSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
