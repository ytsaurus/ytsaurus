#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

#include "row_buffer.h"

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <array>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    T value,
    const TRowBufferPtr& rowBuffer,
    int id,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type*)
{
    if (TEnumTraits<T>::IsBitEnum) {
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
        default:
            THROW_ERROR_EXCEPTION("Cannot parse enum value from %Qlv",
                unversionedValue.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValueImpl(
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
    ToUnversionedValueImpl(
        unversionedValue,
        value,
        NYson::ReflectProtobufMessageType<T>(),
        rowBuffer,
        id);
}

void FromUnversionedValueImpl(
    google::protobuf::Message* value,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    T* value,
    TUnversionedValue unversionedValue,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    FromUnversionedValueImpl(
        value,
        NYson::ReflectProtobufMessageType<T>(),
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TNullable<T>& value,
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
    TNullable<T>* value,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = Null;
    } else {
        value->Emplace();
        FromUnversionedValue(value->GetPtr(), unversionedValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToUnversionedValueImpl(
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
    ToUnversionedValueImpl(
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

void FromUnversionedValueImpl(
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
    FromUnversionedValueImpl(
        [&] {
            values->emplace_back();
            return &values->back();
        },
        NYson::ReflectProtobufMessageType<T>(),
        unversionedValue);
}

void FromUnversionedValueImpl(
    std::function<void(TUnversionedValue)> appender,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    std::vector<T>* values,
    TUnversionedValue unversionedValue,
    typename std::enable_if<TIsScalarPersistentType<T>::Value, void>::type*)
{
    values->clear();
    FromUnversionedValueImpl(
        [&] (TUnversionedValue itemValue) {
            values->emplace_back();
            FromUnversionedValue(&values->back(), itemValue);
        },
        unversionedValue);
}

template <size_t Index, class... Ts>
struct TToUnversionedValuesTraits;

template <size_t Index>
struct TToUnversionedValuesTraits<Index>
{
    template <class V>
    static void Do(V*, const TRowBufferPtr&)
    { }
};

template <size_t Index, class T, class... Ts>
struct TToUnversionedValuesTraits<Index, T, Ts...>
{
    template <class V>
    static void Do(V* array, const TRowBufferPtr& rowBuffer, const T& head, const Ts&... tail)
    {
        ToUnversionedValue(&(*array)[Index], head, rowBuffer);
        TToUnversionedValuesTraits<Index + 1, Ts...>::Do(array, rowBuffer, tail...);
    }
};

template <class... Ts>
auto ToUnversionedValues(
    const TRowBufferPtr& rowBuffer,
    const Ts& ... values)
    -> std::array<TUnversionedValue, sizeof...(Ts)>
{
    std::array<TUnversionedValue, sizeof...(Ts)> array;
    TToUnversionedValuesTraits<0, Ts...>::Do(&array, rowBuffer, values...);
    return array;
}

template <size_t Index, class... Ts>
struct TFromUnversionedRowTraits;

template <size_t Index>
struct TFromUnversionedRowTraits<Index>
{
    static void Do(TUnversionedRow)
    { }
};

template <size_t Index, class T, class... Ts>
struct TFromUnversionedRowTraits<Index, T, Ts...>
{
    static void Do(TUnversionedRow row, T* head, Ts*... tail)
    {
        FromUnversionedValue(head, row[Index]);
        TFromUnversionedRowTraits<Index + 1, Ts...>::Do(row , tail...);
    }
};

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
    TFromUnversionedRowTraits<0, Ts...>::Do(row, values...);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TUnversionedValue ToUnversionedValue(const T& value, const TRowBufferPtr& rowBuffer, int id)
{
    TUnversionedValue unversionedValue;
    ToUnversionedValue(&unversionedValue, value, rowBuffer, id);
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

} // namespace NTableClient
} // namespace NYT
