#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "row_buffer.h"

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/string.h>

#include <array>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////
// Scalar inline types

#define XX(T) \
    template <> \
    struct TUnversionedValueConversionTraits<T, void> \
    { \
        static constexpr bool Scalar = true; \
        static constexpr bool Inline = true; \
    };

XX(i64)
XX(ui64)
XX(i32)
XX(ui32)
XX(i16)
XX(ui16)
XX(i8)
XX(ui8)
XX(bool)
XX(double)
XX(TInstant)

#undef XX

////////////////////////////////////////////////////////////////////////////////
// Scalar non-inline types

#define XX(T) \
    template <> \
    struct TUnversionedValueConversionTraits<T, void> \
    { \
        static constexpr bool Scalar = true; \
        static constexpr bool Inline = false; \
    };

XX(TString)
XX(TStringBuf)
XX(TGuid)

#undef XX

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TUnversionedValueConversionTraits<T, typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type>
{
    static constexpr bool Scalar = true;
    static constexpr bool Inline = !TEnumTraits<T>::IsStringSerializableEnum;
};

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    T value,
    const TRowBufferPtr& rowBuffer,
    int id,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type*)
{
    if constexpr (TEnumTraits<T>::IsStringSerializableEnum) {
        ToUnversionedValue(unversionedValue, NYT::FormatEnum(value), rowBuffer, id);
    } else if constexpr (TEnumTraits<T>::IsBitEnum) {
        ToUnversionedValue(unversionedValue, static_cast<ui64>(value), rowBuffer, id);
    } else {
        ToUnversionedValue(unversionedValue, static_cast<i64>(value), rowBuffer, id);
    }
}

template <class T>
void FromUnversionedValue(
    T* value,
    TUnversionedValue unversionedValue,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type*)
{
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            *value = static_cast<T>(unversionedValue.Data.Int64);
            break;
        case EValueType::Uint64:
            *value = static_cast<T>(unversionedValue.Data.Uint64);
            break;
        case EValueType::String:
            *value = NYT::ParseEnum<T>(TStringBuf(unversionedValue.Data.String, unversionedValue.Length));
            break;
        default:
            THROW_ERROR_EXCEPTION("Cannot parse enum value from %Qlv",
                unversionedValue.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ProtobufToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const google::protobuf::Message& value,
    const NYson::TProtobufMessageType* type,
    const TRowBufferPtr& rowBuffer,
    int id);

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const T& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    ProtobufToUnversionedValueImpl(
        unversionedValue,
        value,
        NYson::ReflectProtobufMessageType<T>(),
        rowBuffer,
        id);
}

////////////////////////////////////////////////////////////////////////////////

void UnversionedValueToProtobufImpl(
    google::protobuf::Message* value,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    T* value,
    TUnversionedValue unversionedValue,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    UnversionedValueToProtobufImpl(
        value,
        NYson::ReflectProtobufMessageType<T>(),
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TUnversionedValueConversionTraits<std::optional<T>, void>
{
    static constexpr bool Scalar = TUnversionedValueConversionTraits<T>::Scalar;
    static constexpr bool Inline = TUnversionedValueConversionTraits<T>::Inline;
};

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const std::optional<T>& value,
    const TRowBufferPtr& rowBuffer,
    int id)
{
    if (value) {
        ToUnversionedValue(unversionedValue, *value, rowBuffer, id);
    } else {
        *unversionedValue = MakeUnversionedSentinelValue(EValueType::Null, id);
    }
}

template <class T>
void FromUnversionedValue(
    std::optional<T>* value,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = std::nullopt;
    } else {
        value->emplace();
        FromUnversionedValue(value->GetPtr(), unversionedValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ListToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const std::function<bool(TUnversionedValue*)> producer,
    const TRowBufferPtr& rowBuffer,
    int id);

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const std::vector<T>& values,
    const TRowBufferPtr& rowBuffer,
    int id)
{
    size_t index = 0;
    ListToUnversionedValueImpl(
        unversionedValue,
        [&] (TUnversionedValue* itemValue) mutable -> bool {
            if (index == values.size()) {
                return false;
            }
            ToUnversionedValue(itemValue, values[index++], rowBuffer);
            return true;
        },
        rowBuffer,
        id);
}

void UnversionedValueToListImpl(
    std::function<google::protobuf::Message*()> appender,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    std::vector<T>* values,
    TUnversionedValue unversionedValue,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    values->clear();
    UnversionedValueToListImpl(
        [&] {
            values->emplace_back();
            return &values->back();
        },
        NYson::ReflectProtobufMessageType<T>(),
        unversionedValue);
}

void UnversionedValueToListImpl(
    std::function<void(TUnversionedValue)> appender,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    std::vector<T>* values,
    TUnversionedValue unversionedValue,
    typename std::enable_if<TUnversionedValueConversionTraits<T>::Scalar, void>::type*)
{
    values->clear();
    UnversionedValueToListImpl(
        [&] (TUnversionedValue itemValue) {
            values->emplace_back();
            FromUnversionedValue(&values->back(), itemValue);
        },
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

void MapToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const std::function<bool(TString*, TUnversionedValue*)> producer,
    const TRowBufferPtr& rowBuffer,
    int id);

template <class TKey, class TValue>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const THashMap<TKey, TValue>& map,
    const TRowBufferPtr& rowBuffer,
    int id)
{
    auto it = map.begin();
    MapToUnversionedValueImpl(
        unversionedValue,
        [&] (TString* itemKey, TUnversionedValue* itemValue) mutable -> bool {
            if (it == map.end()) {
                return false;
            }
            *itemKey = ToString(it->first);
            ToUnversionedValue(itemValue, it->second, rowBuffer);
            ++it;
            return true;
        },
        rowBuffer,
        id);
}

void UnversionedValueToMapImpl(
    std::function<google::protobuf::Message*(TString)> appender,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class TKey, class TValue>
void FromUnversionedValue(
    THashMap<TKey, TValue>* map,
    TUnversionedValue unversionedValue,
    typename std::enable_if<std::is_convertible<TValue*, ::google::protobuf::Message*>::value, void>::type*)
{
    map->clear();
    UnversionedValueToMapImpl(
        [&] (TString key) {
            auto pair = map->insert(std::make_pair(FromString<TKey>(std::move(key)), TValue()));
            return &pair.first->second;
        },
        NYson::ReflectProtobufMessageType<TValue>(),
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
auto ToUnversionedValues(
    const TRowBufferPtr& rowBuffer,
    Ts&&... values)
    -> std::array<TUnversionedValue, sizeof...(Ts)>
{
    std::array<TUnversionedValue, sizeof...(Ts)> array;
    auto* current = array.data();
    (ToUnversionedValue(current++, std::forward<Ts>(values), rowBuffer), ...);
    return array;
}

template <class... Ts>
void FromUnversionedRow(
    TUnversionedRow row,
    Ts*... values)
{
    if (row.GetCount() != sizeof...(Ts)) {
        THROW_ERROR_EXCEPTION("Invalid number of values in row: expected %v, got %v",
            sizeof...(Ts),
            row.GetCount());
    }
    const auto* current = row.Begin();
    (FromUnversionedValue(values, *current++), ...);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TUnversionedValue ToUnversionedValue(T&& value, const TRowBufferPtr& rowBuffer, int id)
{
    TUnversionedValue unversionedValue;
    ToUnversionedValue(&unversionedValue, std::forward<T>(value), rowBuffer, id);
    return unversionedValue;
}

template <class T>
T FromUnversionedValue(TUnversionedValue unversionedValue)
{
    T value;
    FromUnversionedValue(&value, unversionedValue);
    return value;
}

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
TUnversionedOwningRow MakeUnversionedOwningRow(Ts&&... values)
{
    // TODO(babenko): optimize further
    constexpr bool AllTypesInline = (... && TUnversionedValueConversionTraits<Ts>::Inline);
    auto rowBuffer = AllTypesInline ? TRowBufferPtr() : New<TRowBuffer>();
    auto unversionedValues = ToUnversionedValues(rowBuffer, std::forward<Ts>(values)...);
    TUnversionedOwningRowBuilder builder(sizeof...(values));
    int id = 0;
    for (auto& unversionedValue : unversionedValues) {
        unversionedValue.Id = id++;
        builder.AddValue(unversionedValue);
    }
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TRow>
TFuture<void> AsyncReadRows(const TIntrusivePtr<TReader>& reader, std::vector<TRow>* rows)
{
    YT_VERIFY(reader);
    YT_VERIFY(rows);

    rows->clear();
    if (!reader->Read(rows) || !rows->empty()) {
        return VoidFuture;
    }

    return reader->GetReadyEvent().Apply(BIND ([=] () {
        return AsyncReadRows(reader, rows);
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
