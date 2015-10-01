#include "stdafx.h"

#include "value_consumer.h"

#include "name_table.h"
#include "schemaless_writer.h"

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const static i64 MaxBufferSize = (i64) 1 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TBuildingValueConsumer::TBuildingValueConsumer(
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : Schema_(schema)
    , KeyColumns_(keyColumns)
    , NameTable_(TNameTable::FromSchema(Schema_))
    , WrittenFlags_(NameTable_->GetSize(), false)
    , ValueWriter_(&ValueBuffer_)
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
        result.push_back(row.Get());
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
    switch (value.Type) {
        case EValueType::Int64:
            ValueWriter_.OnInt64Scalar(value.Data.Int64);
            break;
        case EValueType::Uint64:
            ValueWriter_.OnUint64Scalar(value.Data.Uint64);
            break;
        case EValueType::Double:
            ValueWriter_.OnDoubleScalar(value.Data.Double);
            break;
        case EValueType::Boolean:
            ValueWriter_.OnBooleanScalar(value.Data.Boolean);
            break;
        case EValueType::String:
            ValueWriter_.OnStringScalar(TStringBuf(value.Data.String, value.Length));
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
        } else if (TreatMissingAsNull_ || id < KeyColumns_.size()) {
            Builder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        }
    }

    std::sort(
        Builder_.BeginValues(),
        Builder_.EndValues(),
        [] (const TUnversionedValue& lhs, const TUnversionedValue& rhs) {
            return lhs.Id < rhs.Id;
        });
    Rows_.emplace_back(Builder_.FinishRow());
}

////////////////////////////////////////////////////////////////////////////////


TWritingValueConsumer::TWritingValueConsumer(ISchemalessWriterPtr writer, bool flushImmediately)
    : Writer_(writer)
    , FlushImmediately_(flushImmediately)
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
    OwningRows_.clear();
    CurrentBufferSize_ = 0;
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
{ }

void TWritingValueConsumer::OnValue(const TUnversionedValue& value)
{
    Builder_.AddValue(value);
}

void TWritingValueConsumer::OnEndRow()
{
    OwningRows_.emplace_back(Builder_.FinishRow());
    const auto& row = OwningRows_.back();

    CurrentBufferSize_ += row.GetSize();
    Rows_.emplace_back(row.Get());

    if (CurrentBufferSize_ > MaxBufferSize || FlushImmediately_) {
        Flush();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
