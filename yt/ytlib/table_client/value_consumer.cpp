#include "value_consumer.h"
#include "name_table.h"
#include "schemaless_writer.h"
#include "row_buffer.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const static i64 MaxBufferSize = (i64) 1 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TBuildingValueConsumer::TBuildingValueConsumer(
    const TTableSchema& schema)
    : Schema_(schema)
    , NameTable_(TNameTable::FromSchema(Schema_))
    , WrittenFlags_(NameTable_->GetSize(), false)
{ }

const std::vector<TUnversionedOwningRow>& TBuildingValueConsumer::GetOwningRows() const
{
    return Rows_;
}

std::vector<TUnversionedRow> TBuildingValueConsumer::GetRows() const
{
    std::vector<TUnversionedRow> result;
    result.reserve(Rows_.size());
    for (const auto& row : Rows_) {
        result.push_back(row);
    }
    return result;
}

void TBuildingValueConsumer::SetTreatMissingAsNull(bool value)
{
    TreatMissingAsNull_ = value;
}

TNameTablePtr TBuildingValueConsumer::GetNameTable() const
{
    return NameTable_;
}

bool TBuildingValueConsumer::GetAllowUnknownColumns() const
{
    return false;
}

void TBuildingValueConsumer::OnBeginRow()
{
    // Do nothing.
}

TUnversionedValue TBuildingValueConsumer::MakeAnyFromScalar(const TUnversionedValue& value)
{
    NYson::TYsonWriter writer(&ValueBuffer_);
    switch (value.Type) {
        case EValueType::Int64:
            writer.OnInt64Scalar(value.Data.Int64);
            break;
        case EValueType::Uint64:
            writer.OnUint64Scalar(value.Data.Uint64);
            break;
        case EValueType::Double:
            writer.OnDoubleScalar(value.Data.Double);
            break;
        case EValueType::Boolean:
            writer.OnBooleanScalar(value.Data.Boolean);
            break;
        case EValueType::String:
            writer.OnStringScalar(TStringBuf(value.Data.String, value.Length));
            break;
        case EValueType::Null:
            writer.OnEntity();
            break;
        default:
            YUNREACHABLE();
    }

    return MakeUnversionedAnyValue(
        TStringBuf(
            ValueBuffer_.Begin(),
            ValueBuffer_.Begin() + ValueBuffer_.Size()),
        value.Id);
}

void TBuildingValueConsumer::OnValue(const TUnversionedValue& value)
{
    auto schemaType = Schema_.Columns()[value.Id].Type;
    if (schemaType == EValueType::Any && value.Type != EValueType::Any) {
        Builder_.AddValue(MakeAnyFromScalar(value));
        ValueBuffer_.Clear();
    } else {
        Builder_.AddValue(value);
    }
    WrittenFlags_[value.Id] = true;
}

void TBuildingValueConsumer::OnEndRow()
{
    for (int id = 0; id < WrittenFlags_.size(); ++id) {
        if (WrittenFlags_[id]) {
            WrittenFlags_[id] = false;
        } else if ((TreatMissingAsNull_ || id < Schema_.GetKeyColumnCount()) && !Schema_.Columns()[id].Expression) {
            Builder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        }
    }

    Rows_.emplace_back(Builder_.FinishRow());
}

////////////////////////////////////////////////////////////////////////////////

TWritingValueConsumer::TWritingValueConsumer(ISchemalessWriterPtr writer, bool flushImmediately)
    : Writer_(writer)
    , FlushImmediately_(flushImmediately)
    , RowBuffer_(New<TRowBuffer>())
{
    YCHECK(Writer_);
}

void TWritingValueConsumer::Flush()
{
    if (!Writer_->Write(Rows_)) {
        auto error = WaitFor(Writer_->GetReadyEvent());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Table writer failed");
    }

    Rows_.clear();
    RowBuffer_->Clear();
}

TNameTablePtr TWritingValueConsumer::GetNameTable() const
{
    return Writer_->GetNameTable();
}

bool TWritingValueConsumer::GetAllowUnknownColumns() const
{
    return true;
}

void TWritingValueConsumer::OnBeginRow()
{
    YASSERT(Values_.empty());
}

void TWritingValueConsumer::OnValue(const TUnversionedValue& value)
{
    Values_.push_back(RowBuffer_->Capture(value));
}

void TWritingValueConsumer::OnEndRow()
{
    auto row = RowBuffer_->Capture(Values_.data(), Values_.size(), false);
    Values_.clear();
    Rows_.push_back(row);

    if (RowBuffer_->GetSize() > MaxBufferSize || FlushImmediately_) {
        Flush();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
