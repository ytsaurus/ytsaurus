#include "data_builder.h"

namespace NYT::NYqlAgent {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDataBuilder::TDataBuilder(IValueConsumer* consumer)
    : ValueConsumer_(consumer)
    , ValueWriter_(&ValueBuffer_)
{ }

void TDataBuilder::OnVoid()
{
    AddNull();
}

void TDataBuilder::OnNull()
{
    AddNull();
}

void TDataBuilder::OnEmptyList()
{
    AddNull();
}

void TDataBuilder::OnEmptyDict()
{
    AddNull();
}

void TDataBuilder::OnBool(bool value)
{
    AddBoolean(value);
}

void TDataBuilder::OnInt8(i8 value)
{
    AddSigned(value);
}

void TDataBuilder::OnUint8(ui8 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnInt16(i16 value)
{
    AddSigned(value);
}

void TDataBuilder::OnUint16(ui16 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnInt32(i32 value)
{
    AddSigned(value);
}

void TDataBuilder::OnUint32(ui32 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnInt64(i64 value)
{
    AddSigned(value);
}

void TDataBuilder::OnUint64(ui64 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnFloat(float value)
{
    AddReal(value);
}

void TDataBuilder::OnDouble(double value)
{
    AddReal(value);
}

void TDataBuilder::OnString(TStringBuf value, bool /*isUtf8*/)
{
    AddString(value);
}

void TDataBuilder::OnUtf8(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnYson(TStringBuf value, bool /*isUtf8*/)
{
    AddYson(value);
}

void TDataBuilder::OnJson(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnJsonDocument(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnUuid(TStringBuf value, bool /*isUtf8*/)
{
    AddString(value);
}

void TDataBuilder::OnDyNumber(TStringBuf value, bool /*isUtf8*/)
{
    AddString(value);
}

void TDataBuilder::OnDate(ui16 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnDatetime(ui32 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnTimestamp(ui64 value)
{
    AddUnsigned(value);
}

void TDataBuilder::OnTzDate(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnTzDatetime(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnTzTimestamp(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnInterval(i64 value)
{
    AddSigned(value);
}

void TDataBuilder::OnDate32(i32 value)
{
    AddSigned(value);
}

void TDataBuilder::OnDatetime64(i64 value)
{
    AddSigned(value);
}

void TDataBuilder::OnTimestamp64(i64 value)
{
    AddSigned(value);
}

void TDataBuilder::OnTzDate32(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnTzDatetime64(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnTzTimestamp64(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnInterval64(i64 value)
{
    AddSigned(value);
}

void TDataBuilder::OnDecimal(TStringBuf value)
{
    AddString(value);
}

void TDataBuilder::OnBeginOptional()
{
}

void TDataBuilder::OnBeforeOptionalItem()
{
}

void TDataBuilder::OnAfterOptionalItem()
{
}

void TDataBuilder::OnEmptyOptional()
{
    AddNull();
}

void TDataBuilder::OnEndOptional()
{
}

void TDataBuilder::OnBeginList()
{
    BeginList();
}

void TDataBuilder::OnBeforeListItem()
{
    NextItem();
    if (Depth_ < 0) {
        ValueConsumer_->OnBeginRow();
    }
}

void TDataBuilder::OnAfterListItem()
{
    if (Depth_ < 0) {
        ValueConsumer_->OnEndRow();
    }
}

void TDataBuilder::OnEndList()
{
    EndList();
}

void TDataBuilder::OnBeginTuple()
{
    BeginList();
}

void TDataBuilder::OnBeforeTupleItem()
{
    NextItem();
}

void TDataBuilder::OnAfterTupleItem()
{
}

void TDataBuilder::OnEndTuple()
{
    EndList();
}

void TDataBuilder::OnBeginStruct()
{
    BeginList();
    if (!Depth_) {
        ColumnIndex_ = 0;
    }
}

void TDataBuilder::OnBeforeStructItem()
{
    NextItem();
}

void TDataBuilder::OnAfterStructItem()
{
    if (!Depth_) {
        ++ColumnIndex_;
    }
}

void TDataBuilder::OnEndStruct()
{
    EndList();
}

void TDataBuilder::OnBeginDict()
{
    BeginList();
}

void TDataBuilder::OnBeforeDictItem()
{
    BeginList();
}

void TDataBuilder::OnBeforeDictKey()
{
    NextItem();
}

void TDataBuilder::OnAfterDictKey()
{
}

void TDataBuilder::OnBeforeDictPayload()
{
    NextItem();
}

void TDataBuilder::OnAfterDictPayload()
{
}

void TDataBuilder::OnAfterDictItem()
{
    EndList();
}

void TDataBuilder::OnEndDict()
{
    EndList();
}

void TDataBuilder::OnBeginVariant(ui32 index)
{
    BeginList();
    NextItem();
    AddSigned(index);
    NextItem();
}

void TDataBuilder::OnEndVariant()
{
    EndList();
}

void TDataBuilder::OnPg(TMaybe<TStringBuf> value, bool /*isUtf8*/)
{
    if (value) {
        AddString(*value);
    } else {
        AddNull();
    }
}

void TDataBuilder::AddNull()
{
    if (Depth_ > 0) {
        ValueWriter_.OnEntity();
    } else {
        ValueConsumer_->OnValue(MakeUnversionedSentinelValue(EValueType::Null, ColumnIndex_));
    }
}

void TDataBuilder::AddBoolean(bool value)
{
    if (Depth_ > 0) {
        ValueWriter_.OnBooleanScalar(value);
    } else {
        ValueConsumer_->OnValue(MakeUnversionedBooleanValue(value, ColumnIndex_));
    }
}

void TDataBuilder::AddSigned(i64 value)
{
    if (Depth_ > 0) {
        ValueWriter_.OnInt64Scalar(value);
    } else {
        ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, ColumnIndex_));
    }
}

void TDataBuilder::AddUnsigned(ui64 value)
{
    if (Depth_ > 0) {
        ValueWriter_.OnUint64Scalar(value);
    } else {
        ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, ColumnIndex_));
    }
}

void TDataBuilder::AddReal(double value)
{
    if (Depth_ > 0) {
        ValueWriter_.OnDoubleScalar(value);
    } else {
        ValueConsumer_->OnValue(MakeUnversionedDoubleValue(value, ColumnIndex_));
    }
}

void TDataBuilder::AddString(TStringBuf value)
{
    if (Depth_ > 0) {
        ValueWriter_.OnStringScalar(value);
    } else {
        ValueConsumer_->OnValue(MakeUnversionedStringValue(value, ColumnIndex_));
    }
}

void TDataBuilder::AddYson(TStringBuf value)
{
    if (Depth_ > 0) {
        ValueWriter_.OnRaw(value, NYson::EYsonType::Node);
    } else {
        ValueConsumer_->OnValue(MakeUnversionedAnyValue(value, ColumnIndex_));
    }
}

void TDataBuilder::BeginList()
{
    if (++Depth_ > 0) {
        ValueWriter_.OnBeginList();
    }
}

void TDataBuilder::NextItem()
{
    if (Depth_ > 0) {
        ValueWriter_.OnListItem();
    }
}

void TDataBuilder::EndList()
{
    if (--Depth_ >= 0) {
        ValueWriter_.OnEndList();

        if (!Depth_) {
            ValueWriter_.Flush();
            const auto accumulatedYson = TStringBuf(ValueBuffer_.Begin(), ValueBuffer_.Begin() + ValueBuffer_.Size());
            ValueConsumer_->OnValue(MakeUnversionedAnyValue(accumulatedYson, ColumnIndex_));
            ValueBuffer_.Clear();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
