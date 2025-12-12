#include "data_builder.h"

#include <yt/yt/library/decimal/decimal.h>

namespace NYT::NYqlAgent {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

TLogicalTypePtr SkipTagged(const TLogicalTypePtr& type)
{
    auto metatype = type->GetMetatype();

    auto skipTaggedOnStructBase = [] (std::vector<TStructField> fields) {
        for (size_t i = 0; i < fields.size(); i++) {
            fields[i].Type = SkipTagged(fields[i].Type);
        }
        return fields;
    };
    auto skipTaggedOnTupleBase = [] (std::vector<TLogicalTypePtr> elements) {
        for (size_t i = 0; i < elements.size(); i++) {
            elements[i] = SkipTagged(elements[i]);
        }
        return elements;
    };

    switch (metatype) {
        case ELogicalMetatype::Simple:
            return SimpleLogicalType(type->AsSimpleTypeRef().GetElement());
        case ELogicalMetatype::Decimal:
            return DecimalLogicalType(type->AsDecimalTypeRef().GetPrecision(), type->AsDecimalTypeRef().GetScale());
        case ELogicalMetatype::Optional:
            return OptionalLogicalType(SkipTagged(type->AsOptionalTypeRef().GetElement()));
        case ELogicalMetatype::List:
            return ListLogicalType(SkipTagged(type->AsListTypeRef().GetElement()));
        case ELogicalMetatype::Struct:
            return StructLogicalType(
                skipTaggedOnStructBase(type->AsStructTypeRef().GetFields()),
                type->AsStructTypeRef().GetRemovedFieldStableNames());
        case ELogicalMetatype::VariantStruct:
            return VariantStructLogicalType(
                skipTaggedOnStructBase(type->AsVariantStructTypeRef().GetFields()));
        case ELogicalMetatype::Tuple:
            return TupleLogicalType(skipTaggedOnTupleBase(type->AsTupleTypeRef().GetElements()));
        case ELogicalMetatype::VariantTuple:
            return VariantTupleLogicalType(skipTaggedOnTupleBase(type->AsVariantTupleTypeRef().GetElements()));
        case ELogicalMetatype::Dict:
            return DictLogicalType(
                SkipTagged(type->AsDictTypeRef().GetKey()),
                SkipTagged(type->AsDictTypeRef().GetValue()));
        case ELogicalMetatype::Tagged:
            return SkipTagged(type->AsTaggedTypeRef().GetElement());
        default:
            THROW_ERROR_EXCEPTION("Invalid metatype in type %v", ToString(*type));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDataBuilder::TDataBuilder(IValueConsumer* consumer, NTableClient::TLogicalTypePtr type)
    : ValueConsumer_(consumer)
    , ValueWriter_(&ValueBuffer_)
{
    Type_.push(SkipTagged(type));
}

void TDataBuilder::OnVoid()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(topType->IsNullable(), "Type %v is not nullable", ToString(*topType));

    AddNull();
}

void TDataBuilder::OnNull()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(topType->IsNullable(), "Type %v is not nullable", ToString(*topType));

    AddNull();
}

void TDataBuilder::OnEmptyList()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(topType->IsNullable(), "Type %v is not nullable", ToString(*topType));

    AddNull();
}

void TDataBuilder::OnEmptyDict()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(topType->IsNullable(), "Type %v is not nullable", ToString(*topType));

    AddNull();
}

void TDataBuilder::OnBool(bool value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Boolean,
        "Type %v is not \"boolean\"",
        ToString(*topType));

    AddBoolean(value);
}

void TDataBuilder::OnInt8(i8 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int8,
        "Type %v is not \"int8\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnUint8(ui8 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint8,
        "Type %v is not \"uint8\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnInt16(i16 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int16,
        "Type %v is not \"int16\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnUint16(ui16 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint16,
        "Type %v is not \"uint16\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnInt32(i32 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int32,
        "Type %v is not \"int32\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnUint32(ui32 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint32,
        "Type %v is not \"uint32\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnInt64(i64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int64,
        "Type %v is not \"int64\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnUint64(ui64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint64,
        "Type %v is not \"uint64\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnFloat(float value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Float,
        "Type %v is not \"float\"",
        ToString(*topType));

    AddReal(value);
}

void TDataBuilder::OnDouble(double value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Double,
        "Type %v is not \"double\"",
        ToString(*topType));

    AddReal(value);
}

void TDataBuilder::OnString(TStringBuf value, bool /*isUtf8*/)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnUtf8(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Utf8,
        "Type %v is not \"utf8\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnYson(TStringBuf value, bool /*isUtf8*/)
{
    AddYson(value);
}

void TDataBuilder::OnJson(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Json,
        "Type %v is not \"json\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnJsonDocument(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnUuid(TStringBuf value, bool /*isUtf8*/)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uuid,
        "Type %v is not \"uuid\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnDyNumber(TStringBuf value, bool /*isUtf8*/)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnDate(ui16 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Date,
        "Type %v is not \"date\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnDatetime(ui32 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Datetime,
        "Type %v is not \"datetime\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnTimestamp(ui64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Timestamp,
        "Type %v is not \"timestamp\"",
        ToString(*topType));

    AddUnsigned(value);
}

void TDataBuilder::OnTzDate(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnTzDatetime(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnTzTimestamp(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnInterval(i64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Interval,
        "Type %v is not \"interval\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnDate32(i32 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Date32,
        "Type %v is not \"date32\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnDatetime64(i64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Datetime64,
        "Type %v is not \"datetime64\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnTimestamp64(i64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Timestamp64,
        "Type %v is not \"timestamp64\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnTzDate32(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnTzDatetime64(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnTzTimestamp64(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type %v is not \"string\"",
        ToString(*topType));

    AddString(value);
}

void TDataBuilder::OnInterval64(i64 value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Simple &&
        topType->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Interval64,
        "Type %v is not \"interval64\"",
        ToString(*topType));

    AddSigned(value);
}

void TDataBuilder::OnDecimal(TStringBuf value)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Decimal,
        "Type %v is not \"decimal\"",
        ToString(*topType));

    std::array<char, NDecimal::TDecimal::MaxBinarySize> buffer;
    AddString(NDecimal::TDecimal::TextToBinary(
        value,
        topType->AsDecimalTypeRef().GetPrecision(),
        topType->AsDecimalTypeRef().GetScale(),
        buffer.data(),
        buffer.size()));
}

void TDataBuilder::OnBeginOptional()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Optional,
        "Type %v is not \"optional\"",
        ToString(*topType));
    Type_.push(topType->AsOptionalTypeRef().GetElement());

    ++OptionalLevels_.top();
}

void TDataBuilder::OnBeforeOptionalItem()
{
    AddBeginOptional();
}

void TDataBuilder::OnAfterOptionalItem()
{
    AddEndOptional();
}

void TDataBuilder::OnEmptyOptional()
{
    AddBeginOptional();
    AddNull();
    AddEndOptional();
}

void TDataBuilder::OnEndOptional()
{
    Type_.pop();
    --OptionalLevels_.top();
}

void TDataBuilder::OnBeginList()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::List,
        "Type %v is not \"list\"",
        ToString(*topType));
    Type_.push(topType->AsListTypeRef().GetElement());

    BeginList();
}

void TDataBuilder::OnBeforeListItem()
{
    OpenItem();
    if (Depth_ < 0) {
        ValueConsumer_->OnBeginRow();
    }
}

void TDataBuilder::OnAfterListItem()
{
    CloseItem();
    if (Depth_ < 0) {
        ValueConsumer_->OnEndRow();
    }
}

void TDataBuilder::OnEndList()
{
    Type_.pop();
    EndList();
}

void TDataBuilder::OnBeginTuple()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Tuple,
        "Type %v is not \"tuple\"",
        ToString(*topType));
    TupleTypes_.push({topType->AsTupleTypeRef().GetElements(), 0});

    BeginList();
}

void TDataBuilder::OnBeforeTupleItem()
{
    THROW_ERROR_EXCEPTION_UNLESS(!TupleTypes_.empty() && TupleTypes_.top().Index < TupleTypes_.top().Types.size(), "Type is not correct Tuple");
    Type_.push(TupleTypes_.top().Types[TupleTypes_.top().Index]);
    OpenItem();
}

void TDataBuilder::OnAfterTupleItem()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        !TupleTypes_.empty(),
        "Type %v is not correct \"tuple\"",
        ToString(*topType));
    TupleTypes_.top().Index++;
    Type_.pop();
    CloseItem();
}

void TDataBuilder::OnEndTuple()
{
    TupleTypes_.pop();
    EndList();
}

void TDataBuilder::OnBeginStruct()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Struct,
        "Type %v is not \"struct\"",
        ToString(*topType));
    StructTypes_.push({topType->AsStructTypeRef().GetFields(), 0});

    BeginList();
    if (!Depth_) {
        ColumnIndex_ = 0;
    }
}

void TDataBuilder::OnBeforeStructItem()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        !StructTypes_.empty() && StructTypes_.top().Index < StructTypes_.top().Types.size(),
        "Type %v is not correct \"struct\"",
        ToString(*topType));
    Type_.push(StructTypes_.top().Types[StructTypes_.top().Index].Type);
    OpenItem();
}

void TDataBuilder::OnAfterStructItem()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        !StructTypes_.empty(),
        "Type %v is not correct \"struct\"",
        ToString(*topType));
    StructTypes_.top().Index++;
    Type_.pop();
    CloseItem();
    if (!Depth_) {
        ++ColumnIndex_;
    }
}

void TDataBuilder::OnEndStruct()
{
    StructTypes_.pop();
    EndList();
}

void TDataBuilder::OnBeginDict()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::Dict,
        "Type %v is not \"dict\"",
        ToString(*topType));
    DictType_.push({topType->AsDictTypeRef().GetKey(), topType->AsDictTypeRef().GetValue()});

    BeginList();
}

void TDataBuilder::OnBeforeDictItem()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        !DictType_.empty(),
        "Type %v is not correct \"dict\"",
        ToString(*topType));
    Type_.push(DictType_.top().Value);
    BeginList();
}

void TDataBuilder::OnBeforeDictKey()
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        !DictType_.empty(),
        "Type %v is not correct \"dict\"",
        ToString(*topType));
    Type_.push(DictType_.top().Key);
    OpenItem();
}

void TDataBuilder::OnAfterDictKey()
{
    Type_.pop();
    CloseItem();
}

void TDataBuilder::OnBeforeDictPayload()
{
    OpenItem();
}

void TDataBuilder::OnAfterDictPayload()
{
    CloseItem();
}

void TDataBuilder::OnAfterDictItem()
{
    Type_.pop();
    EndList();
}

void TDataBuilder::OnEndDict()
{
    DictType_.pop();
    EndList();
}

void TDataBuilder::OnBeginVariant(ui32 index)
{
    const auto& topType = Type_.top();
    THROW_ERROR_EXCEPTION_UNLESS(
        topType->GetMetatype() == ELogicalMetatype::VariantStruct ||
        topType->GetMetatype() == ELogicalMetatype::VariantTuple,
        "Type %v is not \"dict\"",
        ToString(*topType));

    if (topType->GetMetatype() == ELogicalMetatype::VariantStruct) {
        Type_.push(topType->AsVariantStructTypeRef().GetFields()[index].Type);
    } else {
        Type_.push(topType->AsVariantTupleTypeRef().GetElements()[index]);
    }

    BeginList();
    OpenItem();
    CloseItem();
    AddSigned(index);
    OpenItem();
}

void TDataBuilder::OnEndVariant()
{
    Type_.pop();

    CloseItem();
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

void TDataBuilder::AddBeginOptional()
{
    if (OptionalLevels_.top() > 1) {
        BeginList();
    }
}

void TDataBuilder::AddEndOptional()
{
    if (OptionalLevels_.top() > 1) {
        EndList();
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

void TDataBuilder::OpenItem()
{
    OptionalLevels_.push(0);
    if (Depth_ > 0) {
        ValueWriter_.OnListItem();
    }
}

void TDataBuilder::CloseItem()
{
    OptionalLevels_.pop();
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
