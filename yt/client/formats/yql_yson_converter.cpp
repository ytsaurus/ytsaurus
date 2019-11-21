#include "yql_yson_converter.h"

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/json/json_writer.h>

#include <yt/core/yson/pull_parser.h>

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

static TStringBuf TruncateUtf8(TStringBuf string, i64 limit)
{
    auto begin = reinterpret_cast<const unsigned char*>(string.cbegin());
    auto end = reinterpret_cast<const unsigned char*>(string.cend());
    auto it = begin + std::max<i64>(0, limit);
    if (it < end) {
        wchar32 rune;
        size_t runeLength;
        while (it >= begin && SafeReadUTF8Char(rune, runeLength, it, end) != RECODE_OK) {
            --it;
        }
    }
    return string.Trunc(it - begin);
}

TYqlJsonConsumer::TYqlJsonConsumer(IJsonConsumer* underlying)
    : Underlying_(underlying)
{ }

i64 TYqlJsonConsumer::OnInt64Scalar(i64 value)
{
    auto s = ToString(value);
    Underlying_->OnStringScalar(s);
    return GetStringWeight(s);
}

i64 TYqlJsonConsumer::OnUint64Scalar(ui64 value)
{
    auto s = ToString(value);
    Underlying_->OnStringScalar(s);
    return GetStringWeight(s);
}

i64 TYqlJsonConsumer::OnDoubleScalar(double value)
{
    auto s = ::FloatToString(value);
    Underlying_->OnStringScalar(s);
    return GetStringWeight(s);
}

i64 TYqlJsonConsumer::OnBooleanScalar(bool value)
{
    Underlying_->OnBooleanScalar(value);
    return BooleanScalarWeight;
}

i64 TYqlJsonConsumer::TYqlJsonConsumer::OnEntity()
{
    Underlying_->OnEntity();
    return EntityWeight;
}

i64 TYqlJsonConsumer::OnBeginList()
{
    Underlying_->OnBeginList();
    return BeginListWeight;
}

i64 TYqlJsonConsumer::OnListItem()
{
    Underlying_->OnListItem();
    return ListItemWeight;
}

i64 TYqlJsonConsumer::OnEndList()
{
    Underlying_->OnEndList();
    return EndListWeight;
}

i64 TYqlJsonConsumer::OnBeginMap()
{
    Underlying_->OnBeginMap();
    return BeginMapWeight;
}

i64 TYqlJsonConsumer::OnKeyedItem(TStringBuf key)
{
    Underlying_->OnKeyedItem(key);
    return GetKeyedItemWeight(key);
}

i64 TYqlJsonConsumer::OnEndMap()
{
    Underlying_->OnEndMap();
    return EndMapWeight;
}

i64 TYqlJsonConsumer::OnStringScalarWeightLimited(TStringBuf value, i64 limit)
{
    TStringBuf valueToWrite = value;
    auto incomplete = false;
    auto base64 = false;
    if (IsUtf(valueToWrite)) {
        if (static_cast<i64>(valueToWrite.Size()) > limit) {
            valueToWrite = TruncateUtf8(valueToWrite, limit);
            incomplete = true;
        }
    } else {
        base64 = true;
        auto maxEncodedSize = Base64EncodeBufSize(valueToWrite.Size());
        if (static_cast<i64>(maxEncodedSize) > limit) {
            auto truncatedLen = (limit - 1) / 4 * 3;
            incomplete = (truncatedLen < static_cast<i64>(valueToWrite.Size()));
            valueToWrite.Trunc(truncatedLen);
        }
        Buffer_.Clear();
        Buffer_.Resize(Base64EncodeBufSize(valueToWrite.Size()));
        valueToWrite = Base64Encode(valueToWrite, Buffer_.Begin());
    }
    return OnStringScalarImpl(valueToWrite, incomplete, base64);
}

i64 TYqlJsonConsumer::TransferYsonWeightLimited(
    const std::function<void(NYson::IYsonConsumer*)>& callback,
    i64 limit)
{
    Buffer_.Clear();
    TBufferOutput output(Buffer_);
    TYsonWriter writer(&output, EYsonFormat::Text);
    callback(&writer);
    auto valueToWrite = TStringBuf(Buffer_.Begin(), Buffer_.End());
    auto incomplete = false;
    if (static_cast<i64>(Buffer_.Size()) > limit) {
        incomplete = true;
        valueToWrite = {};
    }
    return OnStringScalarImpl(valueToWrite, incomplete);
}

i64 TYqlJsonConsumer::OnStringScalarImpl(
    TStringBuf value,
    bool incomplete,
    bool base64)
{
    i64 weight = 0;
    auto needMap = (incomplete || base64);
    if (needMap) {
        Underlying_->OnBeginMap();
        weight += BeginMapWeight;
    }
    if (incomplete) {
        Underlying_->OnKeyedItem(KeyIncomplete);
        Underlying_->OnBooleanScalar(incomplete);
        weight += GetKeyedItemWeight(KeyIncomplete) + BooleanScalarWeight;
    }
    if (base64) {
        Underlying_->OnKeyedItem(KeyBase64);
        Underlying_->OnBooleanScalar(base64);
        weight += GetKeyedItemWeight(KeyBase64) + BooleanScalarWeight;
    }
    if (needMap) {
        Underlying_->OnKeyedItem(KeyValue);
        weight += GetKeyedItemWeight(KeyValue);
    }
    Underlying_->OnStringScalar(value);
    weight += GetStringWeight(value);
    if (needMap) {
        Underlying_->OnEndMap();
        weight += EndMapWeight;
    }
    return weight;
}

////////////////////////////////////////////////////////////////////////////////

using TWeightLimitedYsonToYqlConverter = std::function<
    i64(NYson::TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)>;
using TWeightLimitedUnversionedValueToYqlConverter = std::function<
    i64(TUnversionedValue value, TYqlJsonConsumer* consumer, i64 weightLimit)>;

static TWeightLimitedYsonToYqlConverter CreateWeightLimitedYsonToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config);

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
    explicit TSimpleYsonToYqlConverter(TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    { }

    i64 operator () (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        i64 stringWeightLimit = std::min(weightLimit, Config_->StringWeightLimit);
        const auto& item = cursor->GetCurrent();
        i64 weight;
        if constexpr (PhysicalType == EValueType::Int64) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::Int64Value);
            weight = consumer->OnInt64Scalar(item.UncheckedAsInt64());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Uint64) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::Uint64Value);
            weight = consumer->OnUint64Scalar(item.UncheckedAsUint64());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::String) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::StringValue);
            weight = consumer->OnStringScalarWeightLimited(
                item.UncheckedAsString(),
                stringWeightLimit);
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Double) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::DoubleValue);
            weight = consumer->OnDoubleScalar(item.UncheckedAsDouble());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Boolean) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::BooleanValue);
            weight = consumer->OnBooleanScalar(item.UncheckedAsBoolean());
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Null) {
            EnsureYsonItemTypeEqual(item, EYsonItemType::EntityValue);
            weight = consumer->OnEntity();
            cursor->Next();
        } else if constexpr (PhysicalType == EValueType::Any) {
            weight = consumer->TransferYsonWeightLimited(
                [cursor] (IYsonConsumer* ysonConsumer) {
                    cursor->TransferComplexValue(ysonConsumer);
                },
                stringWeightLimit);
        } else {
            // Silly assert instead of forbidden |static_assert(false)|.
            static_assert(PhysicalType == EValueType::Int64, "Unexpected physical type");
            // Just to placate the compiler.
            weight = 0;
        }
        return weight;
    }

private:
    TYqlConverterConfigPtr Config_;
};

TWeightLimitedYsonToYqlConverter CreateSimpleTypeYsonToYqlConverter(
    EValueType physicalType,
    TYqlConverterConfigPtr config)
{
    switch (physicalType) {
        case EValueType::Int64:
            return TSimpleYsonToYqlConverter<EValueType::Int64>(std::move(config));
        case EValueType::Uint64:
            return TSimpleYsonToYqlConverter<EValueType::Uint64>(std::move(config));
        case EValueType::String:
            return TSimpleYsonToYqlConverter<EValueType::String>(std::move(config));
        case EValueType::Double:
            return TSimpleYsonToYqlConverter<EValueType::Double>(std::move(config));
        case EValueType::Boolean:
            return TSimpleYsonToYqlConverter<EValueType::Boolean>(std::move(config));
        case EValueType::Null:
            return TSimpleYsonToYqlConverter<EValueType::Null>(std::move(config));
        case EValueType::Any:
            return TSimpleYsonToYqlConverter<EValueType::Any>(std::move(config));
        default:
            YT_ABORT();
    }
}

class TListYsonToYqlConverter
{
public:
    TListYsonToYqlConverter(const TListLogicalType& type, TYqlConverterConfigPtr config)
        : Config_(std::move(config))
        , ElementConverter_(CreateWeightLimitedYsonToYqlConverter(type.GetElement(), Config_))
    { }

    i64 operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
        cursor->Next();

        i64 weight = 0;
        weight += consumer->OnBeginMap();
        weight += consumer->OnKeyedItem(TYqlJsonConsumer::KeyValue);
        weight += consumer->OnBeginList();
        auto incomplete = false;
        while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            if (weight >= weightLimit) {
                incomplete = true;
                break;
            }
            weight += consumer->OnListItem();
            weight += ElementConverter_(cursor, consumer, weightLimit - weight);
        }
        if (incomplete) {
            while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                cursor->SkipComplexValue();
            }
        }
        weight += consumer->OnEndList();
        if (incomplete) {
            weight += consumer->OnKeyedItem(TYqlJsonConsumer::KeyIncomplete);
            weight += consumer->OnBooleanScalar(true);
        }
        weight += consumer->OnEndMap();
        cursor->Next();
        return weight;
    }

private:
    TYqlConverterConfigPtr Config_;
    TWeightLimitedYsonToYqlConverter ElementConverter_;
};

i64 ConvertSequence(
    TYsonPullParserCursor* cursor,
    TYqlJsonConsumer* consumer,
    const std::vector<TWeightLimitedYsonToYqlConverter>& converters,
    i64 weightLimit)
{
    EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
    cursor->Next();

    i64 weight = 0;
    weight += consumer->OnBeginList();
    for (const auto& converter : converters) {
        EnsureYsonItemTypeNotEqual(cursor->GetCurrent(), EYsonItemType::EndList);
        weight += converter(cursor, consumer, weightLimit - weight);
    }
    EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
    weight += consumer->OnEndList();

    cursor->Next();
    return weight;
}

class TStructYsonToYqlConverter
{
public:
    explicit TStructYsonToYqlConverter(const TStructLogicalType& type, TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    {
        for (const auto& field : type.GetFields()) {
            FieldConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(field.Type, Config_));
        }
    }

    i64 operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        return ConvertSequence(cursor, consumer, FieldConverters_, weightLimit);
    }

private:
    TYqlConverterConfigPtr Config_;
    std::vector<TWeightLimitedYsonToYqlConverter> FieldConverters_;
};

class TTupleYsonToYqlConverter
{
public:
    TTupleYsonToYqlConverter(const TTupleLogicalType& type, TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    {
        for (const auto& element : type.GetElements()) {
            ElementConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(element, Config_));
        }
    }

    i64 operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        return ConvertSequence(cursor, consumer, ElementConverters_, weightLimit);
    }

private:
    TYqlConverterConfigPtr Config_;
    std::vector<TWeightLimitedYsonToYqlConverter> ElementConverters_;
};

class TOptionalYsonToYqlConverter
{
public:
    TOptionalYsonToYqlConverter(const TOptionalLogicalType& type, TYqlConverterConfigPtr config)
        : Config_(std::move(config))
        , IsElementNullable_(type.GetElement()->IsNullable())
        , ElementConverter_(CreateWeightLimitedYsonToYqlConverter(type.GetElement(), Config_))
    { }

    i64 operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        i64 weight = 0;
        if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
            weight += consumer->OnEntity();
            cursor->Next();
            return weight;
        }

        weight += consumer->OnBeginList();
        weight += consumer->OnListItem();
        if (IsElementNullable_) {
            EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::BeginList);
            cursor->Next();
            EnsureYsonItemTypeNotEqual(cursor->GetCurrent(), EYsonItemType::EndList);
            weight += ElementConverter_(cursor, consumer, weightLimit - weight);
            EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
            cursor->Next();
        } else {
            weight += ElementConverter_(cursor, consumer, weightLimit - weight);
        }
        weight += consumer->OnEndList();
        return weight;
    }

private:
    TYqlConverterConfigPtr Config_;
    const bool IsElementNullable_;
    const TWeightLimitedYsonToYqlConverter ElementConverter_;
};

class TVariantYsonToYqlConverter
{
public:
    TVariantYsonToYqlConverter(const TVariantTupleLogicalType& type, TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    {
        for (const auto& element : type.GetElements()) {
            ElementConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(element, Config_));
        }
    }

    TVariantYsonToYqlConverter(const TVariantStructLogicalType& type, TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    {
        for (const auto& field : type.GetFields()) {
            ElementConverters_.push_back(CreateWeightLimitedYsonToYqlConverter(field.Type, Config_));
        }
    }

    i64 operator() (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        i64 weight = 0;
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
        weight += consumer->OnBeginList();

        weight += consumer->OnListItem();
        weight += consumer->OnInt64Scalar(alternativeIndex);

        weight += consumer->OnListItem();
        weight += ElementConverters_[alternativeIndex](cursor, consumer, weightLimit - weight);

        EnsureYsonItemTypeEqual(cursor->GetCurrent(), EYsonItemType::EndList);
        weight += consumer->OnEndList();

        cursor->Next();
        return weight;
    }

private:
    TYqlConverterConfigPtr Config_;
    std::vector<TWeightLimitedYsonToYqlConverter> ElementConverters_;
};

static TWeightLimitedYsonToYqlConverter CreateWeightLimitedYsonToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleTypeYsonToYqlConverter(
                GetPhysicalType(logicalType->AsSimpleTypeRef().GetElement()),
                std::move(config));
        case ELogicalMetatype::List:
            return TListYsonToYqlConverter(logicalType->AsListTypeRef(), std::move(config));
        case ELogicalMetatype::Struct:
            return TStructYsonToYqlConverter(logicalType->AsStructTypeRef(), std::move(config));
        case ELogicalMetatype::Optional:
            return TOptionalYsonToYqlConverter(logicalType->AsOptionalTypeRef(), std::move(config));
        case ELogicalMetatype::Tuple:
            return TTupleYsonToYqlConverter(logicalType->AsTupleTypeRef(), std::move(config));
        case ELogicalMetatype::VariantStruct:
            return TVariantYsonToYqlConverter(logicalType->AsVariantStructTypeRef(), std::move(config));
        case ELogicalMetatype::VariantTuple:
            return TVariantYsonToYqlConverter(logicalType->AsVariantTupleTypeRef(), std::move(config));
        case ELogicalMetatype::Dict:
            // Converter is identical to list<tuple<Key, Value>>.
            return TListYsonToYqlConverter(
                TListLogicalType(TupleLogicalType({
                    logicalType->AsDictTypeRef().GetKey(),
                    logicalType->AsDictTypeRef().GetValue()})),
                std::move(config));
        case ELogicalMetatype::Tagged:
            return CreateWeightLimitedYsonToYqlConverter(
                logicalType->AsTaggedTypeRef().GetElement(),
                std::move(config));
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type, bool Required>
class TSimpleUnversionedValueToYqlConverter
{
public:
    explicit TSimpleUnversionedValueToYqlConverter(TYqlConverterConfigPtr config)
        : Config_(std::move(config))
    { }

    i64 operator () (TUnversionedValue value, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        i64 weight = 0;
        if constexpr (!Required) {
            if (value.Type == EValueType::Null) {
                weight += consumer->OnEntity();
                return weight;
            }
            weight += consumer->OnBeginList();
        }

        if (Y_UNLIKELY(value.Type != Type)) {
            THROW_ERROR_EXCEPTION("Bad value type: expected %Qlv, got %Qlv",
                Type,
                value.Type);
        }
        if constexpr (Type == EValueType::Int64) {
            weight += consumer->OnInt64Scalar(value.Data.Int64);
        } else if constexpr (Type == EValueType::Uint64) {
            weight += consumer->OnUint64Scalar(value.Data.Uint64);
        } else if constexpr (Type == EValueType::String) {
            weight += consumer->OnStringScalarWeightLimited(
                TStringBuf(value.Data.String, value.Length),
                weightLimit);
        } else if constexpr (Type == EValueType::Double) {
            weight += consumer->OnDoubleScalar(value.Data.Double);
        } else if constexpr (Type == EValueType::Boolean) {
            weight += consumer->OnBooleanScalar(value.Data.Boolean);
        } else if constexpr (Type == EValueType::Any) {
            weight += consumer->TransferYsonWeightLimited(
                [value] (IYsonConsumer* ysonConsumer) {
                    ParseYsonStringBuffer(TStringBuf(value.Data.String, value.Length), EYsonType::Node, ysonConsumer);
                },
                weightLimit);
        } else if constexpr (Type == EValueType::Null) {
            weight += consumer->OnEntity();
        } else {
            // Silly assert instead of uncompilable |static_assert(false)|.
            static_assert(Type == EValueType::Int64, "Unexpected value type");
        }

        if constexpr (!Required) {
            weight += consumer->OnEndList();
        }
        return weight;
    }

private:
    const TYqlConverterConfigPtr Config_;
};

class TComplexUnversionedValueToYqlConverter
{
public:
    TComplexUnversionedValueToYqlConverter(const TLogicalTypePtr& logicalType, TYqlConverterConfigPtr config)
        : Type_(logicalType)
        , Converter_(CreateWeightLimitedYsonToYqlConverter(logicalType, std::move(config)))
        , IsNullable_(logicalType->IsNullable())
    { }

    i64 operator () (TUnversionedValue value, TYqlJsonConsumer* consumer, i64 weightLimit)
    {
        i64 weight = 0;
        if (value.Type == EValueType::Null) {
            if (Y_UNLIKELY(!IsNullable_)) {
                THROW_ERROR_EXCEPTION("Unexpected value type %Qlv for non-nullable type %Qv",
                    EValueType::Null,
                    NTableClient::ToString(*Type_));
            }
            weight += consumer->OnEntity();
            return weight;
        }
        if (Y_UNLIKELY(value.Type != EValueType::Any)) {
            THROW_ERROR_EXCEPTION("Bad value type: expected %Qlv, got %Qlv",
                EValueType::Any,
                value.Type);
        }
        TMemoryInput input(value.Data.String, value.Length);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        weight += Converter_(&cursor, consumer, weightLimit - weight);
        return weight;
    }

private:
    const TLogicalTypePtr Type_;
    const TWeightLimitedYsonToYqlConverter Converter_;
    const bool IsNullable_;
};

static TWeightLimitedUnversionedValueToYqlConverter CreateSimpleUnversionedValueToYqlConverter(
    EValueType type,
    bool isRequired,
    TYqlConverterConfigPtr config)
{
    switch (type) {
#define CASE(type) \
        case type: \
            if (isRequired) { \
                return TSimpleUnversionedValueToYqlConverter<type, true>(std::move(config)); \
            } else { \
                return TSimpleUnversionedValueToYqlConverter<type, false>(std::move(config)); \
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

static TWeightLimitedUnversionedValueToYqlConverter CreateWeightLimitedUnversionedValueToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config)
{
    auto [maybeSimpleType, isRequired] = SimplifyLogicalType(logicalType);
    if (maybeSimpleType) {
        auto physicalType = GetPhysicalType(*maybeSimpleType);
        return CreateSimpleUnversionedValueToYqlConverter(physicalType, isRequired, std::move(config));
    } else {
        return TComplexUnversionedValueToYqlConverter(logicalType, std::move(config));
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonToYqlConverter CreateYsonToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config)
{
    auto weightLimitedConverter = CreateWeightLimitedYsonToYqlConverter(logicalType, config);
    return [=, config = std::move(config)] (TYsonPullParserCursor* cursor, TYqlJsonConsumer* consumer) {
        weightLimitedConverter(cursor, consumer, config->FieldWeightLimit);
    };
}

TUnversionedValueToYqlConverter CreateUnversionedValueToYqlConverter(
    const TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config)
{
    auto weightLimitedConverter = CreateWeightLimitedUnversionedValueToYqlConverter(logicalType, config);
    return [=, config = std::move(config)] (TUnversionedValue value, TYqlJsonConsumer* consumer) {
        weightLimitedConverter(value, consumer, config->FieldWeightLimit);
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
