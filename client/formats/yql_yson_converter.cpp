#include "yql_yson_converter.h"

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/json/json_writer.h>

#include <yt/core/yson/pull_parser.h>

#include <yt/core/ytree/fluent.h>

#include <library/string_utils/base64/base64.h>

#include <util/charset/utf8.h>

#include <util/string/cast.h>

#include <util/stream/buffer.h>

namespace NYT::NFormats {

using namespace NJson;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYqlJsonConsumer::TYqlJsonConsumer(IJsonConsumer* underlying)
    : Underlying_(underlying)
{ }

void TYqlJsonConsumer::OnStringScalar(TStringBuf value)
{
    if (IsUtf(value)) {
        Underlying_->OnStringScalar(value);
    } else {
        Buffer_.Resize(Base64EncodeBufSize(value.size()));
        Underlying_->OnBeginList();
        Underlying_->OnStringScalar(Base64Encode(value, Buffer_.Begin()));
        Underlying_->OnEndList();
        Buffer_.Clear();
    }
}

void TYqlJsonConsumer::OnInt64Scalar(i64 value)
{
    Underlying_->OnStringScalar(ToString(value));
}

void TYqlJsonConsumer::OnUint64Scalar(ui64 value)
{
    Underlying_->OnStringScalar(ToString(value));
}

void TYqlJsonConsumer::OnDoubleScalar(double value)
{
    Underlying_->OnStringScalar(::FloatToString(value));
}

void TYqlJsonConsumer::OnBooleanScalar(bool value)
{
    Underlying_->OnBooleanScalar(value);
}

void TYqlJsonConsumer::TYqlJsonConsumer::OnEntity()
{
    Underlying_->OnEntity();
}

void TYqlJsonConsumer::OnBeginList()
{
    Underlying_->OnBeginList();
}

void TYqlJsonConsumer::OnListItem()
{
    Underlying_->OnListItem();
}

void TYqlJsonConsumer::OnEndList()
{
    Underlying_->OnEndList();
}

void TYqlJsonConsumer::OnBeginMap()
{
    Underlying_->OnBeginMap();
}

void TYqlJsonConsumer::OnKeyedItem(TStringBuf key)
{
    Underlying_->OnKeyedItem(key);
}

void TYqlJsonConsumer::OnEndMap()
{
    Underlying_->OnEndMap();
}

void TYqlJsonConsumer::OnBeginAttributes()
{
    Underlying_->OnBeginAttributes();
}

void TYqlJsonConsumer::OnEndAttributes() {
    Underlying_->OnEndAttributes();
}

void TYqlJsonConsumer::OnRaw(TStringBuf /* yson */, EYsonType /* type */)
{
    // TYqlJsonConsumer::OnRaw is not supported.
    YT_ABORT();
}

void TYqlJsonConsumer::TransferYson(const std::function<void(IYsonConsumer*)>& callback)
{
    TBufferOutput output(Buffer_);
    TYsonWriter writer(&output, EYsonFormat::Text);
    callback(&writer);
    Underlying_->OnStringScalar(TStringBuf(Buffer_.Begin(), Buffer_.End()));
    Buffer_.Clear();
}

////////////////////////////////////////////////////////////////////////////////

static void EnsureYsonItemTypeEqual(TYsonItem item, EYsonItemType type)
{
    if (Y_UNLIKELY(item.GetType() != type)) {
        THROW_ERROR_EXCEPTION("YSON item type mismatch: expected %Qlv, got %Qlv",
            type,
            item.GetType());
    }
}

static void EnsureYsonItemTypeNotEqual(TYsonItem item, EYsonItemType type)
{
    if (Y_UNLIKELY(item.GetType() == type)) {
        THROW_ERROR_EXCEPTION("Unexpected YSON item type %Qlv",
            type);
    }
}

template <EValueType PhysicalType>
class TSimpleYsonToYqlConverter
{
public:
    void operator () (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer)
    {
        const auto& item = cursor->GetCurrent();
        if constexpr (PhysicalType == EValueType::Int64) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::Int64Value);
            consumer->OnInt64Scalar(item.UncheckedAsInt64());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Uint64) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::Uint64Value);
            consumer->OnUint64Scalar(item.UncheckedAsUint64());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::String) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::StringValue);
            consumer->OnStringScalar(item.UncheckedAsString());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Double) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::DoubleValue);
            consumer->OnDoubleScalar(item.UncheckedAsDouble());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Boolean) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::BooleanValue);
            consumer->OnBooleanScalar(item.UncheckedAsBoolean());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Null) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::EntityValue);
            consumer->OnEntity();
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Any) {
            consumer->TransferYson([cursor] (IYsonConsumer* ysonConsumer) {
                cursor->TransferComplexValue(ysonConsumer);
            });
        } else {
            // Silly assert instead of forbidden |static_assert(false)|.
            static_assert(PhysicalType == EValueType::Int64, "Unexpected physical type");
        }
    }
};

TYsonToYqlConverter CreateSimpleTypeYsonToYqlConverter(EValueType physicalType)
{
    switch (physicalType) {
        case EValueType::Int64:
            return TSimpleYsonToYqlConverter<EValueType::Int64>();
        case EValueType::Uint64:
            return TSimpleYsonToYqlConverter<EValueType::Uint64>();
        case EValueType::String:
            return TSimpleYsonToYqlConverter<EValueType::String>();
        case EValueType::Double:
            return TSimpleYsonToYqlConverter<EValueType::Double>();
        case EValueType::Boolean:
            return TSimpleYsonToYqlConverter<EValueType::Boolean>();
        case EValueType::Null:
            return TSimpleYsonToYqlConverter<EValueType::Null>();
        case EValueType::Any:
            return TSimpleYsonToYqlConverter<EValueType::Any>();
        default:
            YT_ABORT();
    }
}

class TListYsonToYqlConverter
{
public:
    explicit TListYsonToYqlConverter(const TListLogicalType& type)
        : ElementConverter_(CreateYsonToYqlConverter(type.GetElement()))
    { }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer)
    {
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
        cursor->Next();

        BuildYsonFluently(consumer)
            .DoList([&] (TFluentList fluentList) {
                while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                    fluentList
                        .Item().Do([&] (TFluentAny fluent) {
                            ElementConverter_(cursor, consumer);
                        });
                }
            });

        cursor->Next();
    }

private:
    TYsonToYqlConverter ElementConverter_;
};

void ConvertSequence(
    TYsonPullParserCursor* cursor,
    TYqlJsonConsumer* consumer,
    const std::vector<TYsonToYqlConverter>& converters)
{
    EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
    cursor->Next();

    BuildYsonFluently(consumer)
        .DoListFor(converters, [&] (TFluentList fluentList, const TYsonToYqlConverter& converter) {
            EnsureYsonItemTypeNotEqual(cursor->GetCurrent(), EYsonItemType::EndList);
            fluentList
                .Item().Do([&] (TFluentAny fluent) {
                    converter(cursor, consumer);
                });
        });

    EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
    cursor->Next();
}

class TStructYsonToYqlConverter
{
public:
    explicit TStructYsonToYqlConverter(const TStructLogicalType& type)
    {
        for (const auto& field : type.GetFields()) {
            FieldConverters_.push_back(CreateYsonToYqlConverter(field.Type));
        }
    }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer)
    {
        ConvertSequence(cursor, consumer, FieldConverters_);
    }

private:
    std::vector<TYsonToYqlConverter> FieldConverters_;
};

class TTupleYsonToYqlConverter
{
public:
    explicit TTupleYsonToYqlConverter(const TTupleLogicalType& type)
    {
        for (const auto& element : type.GetElements()) {
            ElementConverters_.push_back(CreateYsonToYqlConverter(element));
        }
    }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer)
    {
        ConvertSequence(cursor, consumer, ElementConverters_);
    }

private:
    std::vector<TYsonToYqlConverter> ElementConverters_;
};

class TOptionalYsonToYqlConverter
{
public:
    explicit TOptionalYsonToYqlConverter(const TOptionalLogicalType& type)
        : IsElementNullable_(type.GetElement()->IsNullable())
        , ElementConverter_(CreateYsonToYqlConverter(type.GetElement()))
    { }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer)
    {
        if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
            consumer->OnEntity();
            cursor->Next();
            return;
        }

        BuildYsonFluently(consumer)
            .BeginList()
                .Item().Do([&] (TFluentAny fluent) {
                    if (IsElementNullable_) {
                        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
                        cursor->Next();
                        EnsureYsonItemTypeNotEqual(cursor->GetCurrent(), EYsonItemType::EndList);
                        ElementConverter_(cursor, consumer);
                        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
                        cursor->Next();
                    } else {
                        ElementConverter_(cursor, consumer);
                    }
                })
            .EndList();
    }

private:
    const bool IsElementNullable_;
    const TYsonToYqlConverter ElementConverter_;
};

class TVariantYsonToYqlConverter
{
public:
    explicit TVariantYsonToYqlConverter(const TVariantTupleLogicalType& type)
    {
        for (const auto& element : type.GetElements()) {
            ElementConverters_.push_back(CreateYsonToYqlConverter(element));
        }
    }

    explicit TVariantYsonToYqlConverter(const TVariantStructLogicalType& type)
    {
        for (const auto& field : type.GetFields()) {
            ElementConverters_.push_back(CreateYsonToYqlConverter(field.Type));
        }
    }

    void operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer)
    {
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
        cursor->Next();
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::Int64Value);
        const auto alternativeIndex = cursor->GetCurrent().UncheckedAsInt64();
        if (Y_UNLIKELY(!(0 <= alternativeIndex && alternativeIndex < static_cast<int>(ElementConverters_.size())))) {
            THROW_ERROR_EXCEPTION("Alternative index is out of bounds: expected it to be in [%v, %v), got %v",
                0,
                ElementConverters_.size(),
                alternativeIndex);
        }
        cursor->Next();
        BuildYsonFluently(consumer)
            .BeginList()
                .Item().Value(ToString(alternativeIndex))
                .Item().Do([&] (TFluentAny fluent) {
                    ElementConverters_[alternativeIndex](cursor, consumer);
                })
            .EndList();
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
        cursor->Next();
    }

private:
    std::vector<TYsonToYqlConverter> ElementConverters_;
};

TYsonToYqlConverter CreateYsonToYqlConverter(const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleTypeYsonToYqlConverter(
                GetPhysicalType(logicalType->AsSimpleTypeRef().GetElement()));
        case ELogicalMetatype::List:
            return TListYsonToYqlConverter(logicalType->AsListTypeRef());
        case ELogicalMetatype::Struct:
            return TStructYsonToYqlConverter(logicalType->AsStructTypeRef());
        case ELogicalMetatype::Optional:
            return TOptionalYsonToYqlConverter(logicalType->AsOptionalTypeRef());
        case ELogicalMetatype::Tuple:
            return TTupleYsonToYqlConverter(logicalType->AsTupleTypeRef());
        case ELogicalMetatype::VariantStruct:
            return TVariantYsonToYqlConverter(logicalType->AsVariantStructTypeRef());
        case ELogicalMetatype::VariantTuple:
            return TVariantYsonToYqlConverter(logicalType->AsVariantTupleTypeRef());
        case ELogicalMetatype::Dict:
            // Converter is identical to list<tuple<Key, Value>>.
            return TListYsonToYqlConverter(TListLogicalType(TupleLogicalType({
                logicalType->AsDictTypeRef().GetKey(),
                logicalType->AsDictTypeRef().GetValue()})));
        case ELogicalMetatype::Tagged:
            return CreateYsonToYqlConverter(logicalType->AsTaggedTypeRef().GetElement());
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type, bool Required>
class TSimpleUnversionedValueToYqlConverter
{
public:
    void operator () (TUnversionedValue value, TYqlJsonConsumer* consumer)
    {
        if constexpr (!Required) {
            if (value.Type == EValueType::Null) {
                consumer->OnEntity();
                return;
            }
            consumer->OnBeginList();
        }

        if (Y_UNLIKELY(value.Type != Type)) {
            THROW_ERROR_EXCEPTION("Bad value Type: expected %Qlv, got %Qlv",
                Type,
                value.Type);
        }

        if constexpr (Type == EValueType::Int64) {
            consumer->OnInt64Scalar(value.Data.Int64);
        } else if constexpr (Type == EValueType::Uint64) {
            consumer->OnUint64Scalar(value.Data.Uint64);
        } else if constexpr (Type == EValueType::String) {
            consumer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
        } else if constexpr (Type == EValueType::Double) {
            consumer->OnDoubleScalar(value.Data.Double);
        } else if constexpr (Type == EValueType::Boolean) {
            consumer->OnBooleanScalar(value.Data.Boolean);
        } else if constexpr (Type == EValueType::Any) {
            consumer->TransferYson([value] (IYsonConsumer* ysonConsumer) {
                ParseYsonStringBuffer(TStringBuf(value.Data.String, value.Length), EYsonType::Node, ysonConsumer);
            });
        } else if constexpr (Type == EValueType::Null) {
            consumer->OnEntity();
        } else {
            // Silly assert instead of uncompilable |static_assert(false)|.
            static_assert(Type == EValueType::Int64, "Unexpected value type");
        }

        if constexpr (!Required) {
            consumer->OnEndList();
        }
    }
};

class TComplexUnversionedValueToYqlConverter
{
public:
    explicit TComplexUnversionedValueToYqlConverter(const TLogicalTypePtr& logicalType)
        : Type_(logicalType)
        , Converter_(CreateYsonToYqlConverter(logicalType))
        , IsNullable_(logicalType->IsNullable())
    { }

    void operator () (TUnversionedValue value, TYqlJsonConsumer* consumer)
    {
        if (value.Type == EValueType::Null) {
            if (Y_UNLIKELY(!IsNullable_)) {
                THROW_ERROR_EXCEPTION("Unexpected value type %Qlv for non-nullable type %Qv",
                    EValueType::Null,
                    NTableClient::ToString(*Type_));
            }
            consumer->OnEntity();
            return;
        }
        if (Y_UNLIKELY(value.Type != EValueType::Any)) {
            THROW_ERROR_EXCEPTION("Bad value type: expected %Qlv, got %Qlv",
                EValueType::Any,
                value.Type);
        }
        TMemoryInput input(value.Data.String, value.Length);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        Converter_(&cursor, consumer);
    }

private:
    const TLogicalTypePtr Type_;
    const TYsonToYqlConverter Converter_;
    const bool IsNullable_;
};

TUnversionedValueToYqlConverter CreateSimpleUnversionedValueToYqlConverter(EValueType type, bool isRequired)
{
    switch (type) {
#define CASE(type) \
        case type: \
            if (isRequired) { \
                return TSimpleUnversionedValueToYqlConverter<type, true>(); \
            } else { \
                return TSimpleUnversionedValueToYqlConverter<type, false>(); \
            }

        CASE(EValueType::Int64)
        CASE(EValueType::Uint64)
        CASE(EValueType::String)
        CASE(EValueType::Double)
        CASE(EValueType::Boolean)
        CASE(EValueType::Any)
        CASE(EValueType::Null)
#undef CASE

        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    YT_ABORT();
}

TUnversionedValueToYqlConverter CreateUnversionedValueToYqlConverter(const TLogicalTypePtr& logicalType)
{
    auto [maybeSimpleType, isRequired] = SimplifyLogicalType(logicalType);
    if (maybeSimpleType) {
        auto physicalType = GetPhysicalType(*maybeSimpleType);
        return CreateSimpleUnversionedValueToYqlConverter(physicalType, isRequired);
    } else {
        return TComplexUnversionedValueToYqlConverter(logicalType);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
