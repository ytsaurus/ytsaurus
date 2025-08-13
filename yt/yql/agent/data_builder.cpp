#include "data_builder.h"

#include <yt/yt/library/decimal/decimal.h>

namespace NYT::NYqlAgent {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr SkipTagged(const TLogicalTypePtr& type)
{
    const auto metatype = type->GetMetatype();

    const auto skipTaggedOnStructBase = [] (std::vector<TStructField> fields) {
        for (size_t i = 0; i < fields.size(); i++) {
            fields[i].Type = SkipTagged(fields[i].Type);
        }
        return fields;
    };
    const auto skipTaggedOnTupleBase = [] (std::vector<TLogicalTypePtr> elements) {
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
            return StructLogicalType(skipTaggedOnStructBase(type->AsStructTypeRef().GetFields()));
        case ELogicalMetatype::VariantStruct:
            return VariantStructLogicalType(skipTaggedOnStructBase(type->AsVariantStructTypeRef().GetFields()));
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
            THROW_ERROR_EXCEPTION("Invalid metatype");
    }
}

////////////////////////////////////////////////////////////////////////////////

TDataBuilder::TDataBuilder(IValueConsumer* consumer, NTableClient::TLogicalTypePtr type)
    : ValueConsumer_(consumer)
    , ValueWriter_(&ValueBuffer_)
{
    Type_.push(SkipTagged(type));
}

void TDataBuilder::OnVoid()
{
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->IsNullable(), "Type is not nullable");

    AddNull();
}

void TDataBuilder::OnNull()
{
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->IsNullable(), "Type is not nullable");

    AddNull();
}

void TDataBuilder::OnEmptyList()
{
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->IsNullable(), "Type is not nullable");

    AddNull();
}

void TDataBuilder::OnEmptyDict()
{
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->IsNullable(), "Type is not nullable");

    AddNull();
}

void TDataBuilder::OnBool(bool value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Boolean,
        "Type is not Boolean");

    AddBoolean(value);
}

void TDataBuilder::OnInt8(i8 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int8,
        "Type is not Int8");

    AddSigned(value);
}

void TDataBuilder::OnUint8(ui8 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint8,
        "Type is not Uint8");

    AddUnsigned(value);
}

void TDataBuilder::OnInt16(i16 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int16,
        "Type is not Int16");

    AddSigned(value);
}

void TDataBuilder::OnUint16(ui16 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint16,
        "Type is not Uint16");

    AddUnsigned(value);
}

void TDataBuilder::OnInt32(i32 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int32,
        "Type is not Int32");

    AddSigned(value);
}

void TDataBuilder::OnUint32(ui32 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint32,
        "Type is not Uint32");

    AddUnsigned(value);
}

void TDataBuilder::OnInt64(i64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Int64,
        "Type is not Int64");

    AddSigned(value);
}

void TDataBuilder::OnUint64(ui64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uint64,
        "Type is not Uint64");

    AddUnsigned(value);
}

void TDataBuilder::OnFloat(float value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Float,
        "Type is not Float");

    AddReal(value);
}

void TDataBuilder::OnDouble(double value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Double,
        "Type is not Double");

    AddReal(value);
}

void TDataBuilder::OnString(TStringBuf value, bool /*isUtf8*/)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnUtf8(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Utf8,
        "Type is not Utf8");

    AddString(value);
}

void TDataBuilder::OnYson(TStringBuf value, bool /*isUtf8*/)
{
    AddYson(value);
}

void TDataBuilder::OnJson(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Json,
        "Type is not Json");

    AddString(value);
}

void TDataBuilder::OnJsonDocument(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnUuid(TStringBuf value, bool /*isUtf8*/)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Uuid,
        "Type is not Uuid");

    AddString(value);
}

void TDataBuilder::OnDyNumber(TStringBuf value, bool /*isUtf8*/)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnDate(ui16 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Date,
        "Type is not Date");

    AddUnsigned(value);
}

void TDataBuilder::OnDatetime(ui32 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Datetime,
        "Type is not Datetime");

    AddUnsigned(value);
}

void TDataBuilder::OnTimestamp(ui64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Timestamp,
        "Type is not Timestamp");

    AddUnsigned(value);
}

void TDataBuilder::OnTzDate(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnTzDatetime(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnTzTimestamp(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnInterval(i64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Interval,
        "Type is not Interval");

    AddSigned(value);
}

void TDataBuilder::OnDate32(i32 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Date32,
        "Type is not Date32");

    AddSigned(value);
}

void TDataBuilder::OnDatetime64(i64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Datetime64,
        "Type is not Datetime64");

    AddSigned(value);
}

void TDataBuilder::OnTimestamp64(i64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Timestamp64,
        "Type is not Timestamp64");

    AddSigned(value);
}

void TDataBuilder::OnTzDate32(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnTzDatetime64(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnTzTimestamp64(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::String,
        "Type is not String");

    AddString(value);
}

void TDataBuilder::OnInterval64(i64 value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Simple &&
        Type_.top()->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Interval64,
        "Type is not Interval64");

    AddSigned(value);
}

void TDataBuilder::OnDecimal(TStringBuf value)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::Decimal,
        "Type is not Decimal");

    std::array<char, NDecimal::TDecimal::MaxBinarySize> buffer;
    AddString(NDecimal::TDecimal::TextToBinary(
        value,
        Type_.top()->AsDecimalTypeRef().GetPrecision(),
        Type_.top()->AsDecimalTypeRef().GetScale(),
        buffer.data(),
        buffer.size()));
}

void TDataBuilder::OnBeginOptional()
{
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->GetMetatype() == ELogicalMetatype::Optional, "Type is not Optional");
    Type_.push(Type_.top()->AsOptionalTypeRef().GetElement());

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
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->GetMetatype() == ELogicalMetatype::List, "Type is not List");
    Type_.push(Type_.top()->AsListTypeRef().GetElement());

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
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->GetMetatype() == ELogicalMetatype::Tuple, "Type is not Tuple");
    TupleTypes_.push({Type_.top()->AsTupleTypeRef().GetElements(), 0});

    BeginList();
}

void TDataBuilder::OnBeforeTupleItem()
{
    THROW_ERROR_EXCEPTION_UNLESS(!TupleTypes_.empty() && TupleTypes_.top().Index_ < TupleTypes_.top().Types_.size(), "Type is not correct Tuple");
    Type_.push(TupleTypes_.top().Types_[TupleTypes_.top().Index_]);
    OpenItem();
}

void TDataBuilder::OnAfterTupleItem()
{
    THROW_ERROR_EXCEPTION_UNLESS(!TupleTypes_.empty(), "Type is not correct Tuple");
    TupleTypes_.top().Index_++;
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
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->GetMetatype() == ELogicalMetatype::Struct, "Type is not Struct");
    StructTypes_.push({Type_.top()->AsStructTypeRef().GetFields(), 0});

    BeginList();
    if (!Depth_) {
        ColumnIndex_ = 0;
    }
}

void TDataBuilder::OnBeforeStructItem()
{
    THROW_ERROR_EXCEPTION_UNLESS(!StructTypes_.empty() && StructTypes_.top().Index_ < StructTypes_.top().Types_.size(), "Type is not correct Struct");
    Type_.push(StructTypes_.top().Types_[StructTypes_.top().Index_].Type);
    OpenItem();
}

void TDataBuilder::OnAfterStructItem()
{
    THROW_ERROR_EXCEPTION_UNLESS(!StructTypes_.empty(), "Type is not correct Struct");
    StructTypes_.top().Index_++;
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
    THROW_ERROR_EXCEPTION_UNLESS(Type_.top()->GetMetatype() == ELogicalMetatype::Dict, "Type is not Dict");
    DictType_.push({Type_.top()->AsDictTypeRef().GetKey(), Type_.top()->AsDictTypeRef().GetValue()});

    BeginList();
}

void TDataBuilder::OnBeforeDictItem()
{
    THROW_ERROR_EXCEPTION_UNLESS(!DictType_.empty(), "Type is not correct Dict");
    Type_.push(DictType_.top().Value_);
    BeginList();
}

void TDataBuilder::OnBeforeDictKey()
{
    THROW_ERROR_EXCEPTION_UNLESS(!DictType_.empty(), "Type is not correct Dict");
    Type_.push(DictType_.top().Key_);
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
    THROW_ERROR_EXCEPTION_UNLESS(
        Type_.top()->GetMetatype() == ELogicalMetatype::VariantStruct
            || Type_.top()->GetMetatype() == ELogicalMetatype::VariantTuple,
        "Type is not Dict");

    if (Type_.top()->GetMetatype() == ELogicalMetatype::VariantStruct) {
        Type_.push(Type_.top()->AsVariantStructTypeRef().GetFields()[index].Type);
    } else {
        Type_.push(Type_.top()->AsVariantTupleTypeRef().GetElements()[index]);
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
