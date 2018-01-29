#include "value_consumer.h"

#include "helpers.h"
#include "name_table.h"
#include "row_buffer.h"
#include "schemaless_writer.h"

#include <yt/core/concurrency/scheduler.h>

#include <util/string/cast.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool ConvertToBooleanValue(TStringBuf stringValue)
{
    if (stringValue == "true") {
        return true;
    } else if (stringValue == "false") {
        return false;
    } else {
        THROW_ERROR_EXCEPTION("Unable to convert value to boolean")
            << TErrorAttribute("value", stringValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

TValueConsumerBase::TValueConsumerBase(
    const TTableSchema& schema,
    const TTypeConversionConfigPtr& typeConversionConfig)
    : Schema_(schema)
    , TypeConversionConfig_(typeConversionConfig)
{ }

void TValueConsumerBase::InitializeIdToTypeMapping()
{
    const auto& nameTable = GetNameTable();
    for (const auto& column : Schema_.Columns()) {
        int id = nameTable->GetIdOrRegisterName(column.Name());
        if (id >= static_cast<int>(NameTableIdToType_.size())) {
            NameTableIdToType_.resize(id + 1, EValueType::Any);
        }
        NameTableIdToType_[id] = column.GetPhysicalType();
    }
}

template <typename T>
void TValueConsumerBase::ProcessIntegralValue(const TUnversionedValue& value, EValueType columnType)
{
    T integralValue;
    GetValue(&integralValue, value);

    if (TypeConversionConfig_->EnableAllToStringConversion && columnType == EValueType::String) {
        char buf[64];
        char* end = buf + 64;
        char* start = WriteIntToBufferBackwards(end, integralValue);
        OnMyValue(MakeUnversionedStringValue(TStringBuf(start, end), value.Id));
    } else if (TypeConversionConfig_->EnableIntegralToDoubleConversion && columnType == EValueType::Double) {
        OnMyValue(MakeUnversionedDoubleValue(static_cast<double>(integralValue), value.Id));
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::ProcessInt64Value(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableIntegralTypeConversion && columnType == EValueType::Uint64) {
        i64 integralValue = value.Data.Int64;
        if (integralValue < 0) {
            ThrowConversionException(
                value,
                columnType,
                TError("Unable to convert negative int64 to uint64")
                    << TErrorAttribute("value", integralValue));
        } else {
            OnMyValue(MakeUnversionedUint64Value(static_cast<ui64>(integralValue), value.Id));
        }
    } else {
        ProcessIntegralValue<i64>(value, columnType);
    }
}

void TValueConsumerBase::ProcessUint64Value(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableIntegralTypeConversion && columnType == EValueType::Int64) {
        ui64 integralValue = value.Data.Uint64;
        if (integralValue > std::numeric_limits<i64>::max()) {
            ThrowConversionException(
                value,
                columnType,
                TError("Unable to convert uint64 to int64 as it leads to an overflow")
                    << TErrorAttribute("value", integralValue));
        } else {
            OnMyValue(MakeUnversionedInt64Value(static_cast<i64>(integralValue), value.Id));
        }
    } else {
        ProcessIntegralValue<ui64>(value, columnType);
    }
}

void TValueConsumerBase::ProcessBooleanValue(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableAllToStringConversion && columnType == EValueType::String) {
        TStringBuf stringValue = value.Data.Boolean ? "true" : "false";
        OnMyValue(MakeUnversionedStringValue(stringValue, value.Id));
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::ProcessDoubleValue(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableAllToStringConversion && columnType == EValueType::String) {
        char buf[64];
        auto length = FloatToString(value.Data.Double, buf, sizeof(buf));
        TStringBuf stringValue(buf, buf + length);
        OnMyValue(MakeUnversionedStringValue(stringValue, value.Id));
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::ProcessStringValue(const TUnversionedValue& value, EValueType columnType)
{
    if (TypeConversionConfig_->EnableStringToAllConversion) {
        TUnversionedValue convertedValue;
        TStringBuf stringValue(value.Data.String, value.Length);
        try {
            switch (columnType) {
                case EValueType::Int64:
                    convertedValue = MakeUnversionedInt64Value(FromString<i64>(stringValue), value.Id);
                    break;
                case EValueType::Uint64:
                    convertedValue = MakeUnversionedUint64Value(FromString<ui64>(stringValue), value.Id);
                    break;
                case EValueType::Double:
                    convertedValue = MakeUnversionedDoubleValue(FromString<double>(stringValue), value.Id);
                    break;
                case EValueType::Boolean:
                    convertedValue = MakeUnversionedBooleanValue(ConvertToBooleanValue(stringValue), value.Id);
                    break;
                default:
                    convertedValue = value;
                    break;
            }
        } catch (const std::exception& ex) {
            ThrowConversionException(value, columnType, TError(ex));
        }
        OnMyValue(convertedValue);
    } else {
        OnMyValue(value);
    }
}

void TValueConsumerBase::OnValue(const TUnversionedValue& value)
{
    EValueType columnType;
    if (NameTableIdToType_.size() <= value.Id) {
        columnType = EValueType::Any;
    } else {
        columnType = NameTableIdToType_[value.Id];
    }

    switch (value.Type) {
        case EValueType::Int64:
            ProcessInt64Value(value, columnType);
            break;
        case EValueType::Uint64:
            ProcessUint64Value(value, columnType);
            break;
        case EValueType::Boolean:
            ProcessBooleanValue(value, columnType);
            break;
        case EValueType::Double:
            ProcessDoubleValue(value, columnType);
            break;
        case EValueType::String:
            ProcessStringValue(value, columnType);
            break;
        default:
            OnMyValue(value);
            break;
    }
}

void TValueConsumerBase::ThrowConversionException(const TUnversionedValue& value, EValueType columnType, const TError& ex)
{
    THROW_ERROR_EXCEPTION("Error while performing type conversion")
        << ex
        << TErrorAttribute("column", GetNameTable()->GetName(value.Id))
        << TErrorAttribute("value_type", value.Type)
        << TErrorAttribute("column_type", columnType);
}

////////////////////////////////////////////////////////////////////////////////

static const i64 MaxBufferSize = 1_MB;

////////////////////////////////////////////////////////////////////////////////

TBuildingValueConsumer::TBuildingValueConsumer(
    const TTableSchema& schema,
    const TTypeConversionConfigPtr& typeConversionConfig)
    : TValueConsumerBase(schema, typeConversionConfig)
    , NameTable_(TNameTable::FromSchema(Schema_))
    , WrittenFlags_(NameTable_->GetSize(), false)
{
    InitializeIdToTypeMapping();
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

void TBuildingValueConsumer::SetAggregate(bool value)
{
    Aggregate_ = value;
}

void TBuildingValueConsumer::SetTreatMissingAsNull(bool value)
{
    TreatMissingAsNull_ = value;
}

const TNameTablePtr& TBuildingValueConsumer::GetNameTable() const
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
    NYson::TBufferedBinaryYsonWriter writer(&ValueBuffer_);
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
            Y_UNREACHABLE();
    }
    writer.Flush();

    return MakeUnversionedAnyValue(
        TStringBuf(
            ValueBuffer_.Begin(),
            ValueBuffer_.Begin() + ValueBuffer_.Size()),
        value.Id,
        value.Aggregate);
}

void TBuildingValueConsumer::OnMyValue(const TUnversionedValue& value)
{
    if (value.Id >= Schema_.Columns().size()) {
        return;
    }
    auto valueCopy = value;
    const auto& columnSchema = Schema_.Columns()[valueCopy.Id];
    if (columnSchema.Aggregate()) {
        valueCopy.Aggregate = Aggregate_;
    }
    if (columnSchema.GetPhysicalType() == EValueType::Any && valueCopy.Type != EValueType::Any) {
        Builder_.AddValue(MakeAnyFromScalar(valueCopy));
        ValueBuffer_.Clear();
    } else {
        Builder_.AddValue(valueCopy);
    }
    WrittenFlags_[valueCopy.Id] = true;
}

void TBuildingValueConsumer::OnEndRow()
{
    for (int id = 0; id < WrittenFlags_.size(); ++id) {
        if (WrittenFlags_[id]) {
            WrittenFlags_[id] = false;
        } else if ((TreatMissingAsNull_ || id < Schema_.GetKeyColumnCount()) && !Schema_.Columns()[id].Expression()) {
            Builder_.AddValue(MakeUnversionedSentinelValue(
                EValueType::Null,
                id,
                Schema_.Columns()[id].Aggregate() && Aggregate_));
        }
    }

    Rows_.emplace_back(Builder_.FinishRow());
}

////////////////////////////////////////////////////////////////////////////////

struct TWritingValueConsumerBufferTag
{ };

TWritingValueConsumer::TWritingValueConsumer(
    ISchemalessWriterPtr writer,
    const TTypeConversionConfigPtr& typeConversionConfig,
    bool flushImmediately)
    : TValueConsumerBase(writer->GetSchema(), typeConversionConfig)
    , Writer_(writer)
    , FlushImmediately_(flushImmediately)
    , RowBuffer_(New<TRowBuffer>(TWritingValueConsumerBufferTag()))
{
    YCHECK(Writer_);
    InitializeIdToTypeMapping();
}

TFuture<void> TWritingValueConsumer::Flush()
{
    return Writer_->GetReadyEvent()
        .Apply(BIND([=, rows = std::move(Rows_)] () {
            Writer_->Write(rows);
            RowBuffer_->Clear();
        }));
}

const TNameTablePtr& TWritingValueConsumer::GetNameTable() const
{
    return Writer_->GetNameTable();
}

bool TWritingValueConsumer::GetAllowUnknownColumns() const
{
    return true;
}

void TWritingValueConsumer::OnBeginRow()
{
    Y_ASSERT(Values_.empty());
}

void TWritingValueConsumer::OnMyValue(const TUnversionedValue& value)
{
    Values_.push_back(RowBuffer_->Capture(value));
}

void TWritingValueConsumer::OnEndRow()
{
    auto row = RowBuffer_->Capture(Values_.data(), Values_.size(), false);
    Values_.clear();
    Rows_.push_back(row);

    if (RowBuffer_->GetSize() > MaxBufferSize || FlushImmediately_) {
        auto error = WaitFor(Flush());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Table writer failed")
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
