#pragma once

#include <yt/yt/core/misc/error.h>

#ifndef PULL_PARSER_DESERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include pull_parser_deserialize.h"
// For the sake of sane code completion.
#include "pull_parser_deserialize.h"
#endif

#include "public.h"

#include "pull_parser.h"
#include "pull_parser_deserialize.h"

#include <vector>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline void SkipAttributes(TYsonPullParserCursor* cursor)
{
    cursor->SkipAttributes();
    if ((*cursor)->GetType() == EYsonItemType::BeginAttributes) {
        THROW_ERROR_EXCEPTION("Repeated attributes are not allowed");
    }
}

inline void MaybeSkipAttributes(TYsonPullParserCursor* cursor)
{
    if ((*cursor)->GetType() == EYsonItemType::BeginAttributes) {
        SkipAttributes(cursor);
    }
}

template <class T>
void DeserializeVector(T& value, TYsonPullParserCursor* cursor) {
    NDetail::MaybeSkipAttributes(cursor);
    int index = 0;
    cursor->ParseList([&](TYsonPullParserCursor* cursor) {
        if (index < static_cast<int>(value.size())) {
            Deserialize(value[index], cursor);
        } else {
            value.emplace_back();
            Deserialize(value.back(), cursor);
        }
        ++index;
    });
    value.resize(index);
}

template <class T>
void DeserializeSet(T& value, TYsonPullParserCursor* cursor)
{
    NDetail::MaybeSkipAttributes(cursor);
    value.clear();
    cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
        value.insert(ExtractTo<typename T::value_type>(cursor));
    });
}

template <class T>
void DeserializeMap(T& value, TYsonPullParserCursor* cursor)
{
    NDetail::MaybeSkipAttributes(cursor);
    value.clear();
    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        auto key = ExtractTo<typename T::key_type>(cursor);
        auto item = ExtractTo<typename T::mapped_type>(cursor);
        if (value.contains(key)) {
            THROW_ERROR_EXCEPTION("Duplicate key %Qv", key);
        }
        value.emplace(std::move(key), std::move(item));
    });
}

template <class T, bool IsSet = std::is_same<typename T::key_type, typename T::value_type>::value>
struct TAssociativeHelper;

template <class T>
struct TAssociativeHelper<T, true>
{
    static void Deserialize(T& value, TYsonPullParserCursor* cursor)
    {
        DeserializeSet(value, cursor);
    }
};

template <class T>
struct TAssociativeHelper<T, false>
{
    static void Deserialize(T& value, TYsonPullParserCursor* cursor)
    {
        DeserializeMap(value, cursor);
    }
};

template <class T>
void DeserializeAssociative(T& value, TYsonPullParserCursor* cursor)
{
    TAssociativeHelper<T>::Deserialize(value, cursor);
}

template <class T, size_t Size = std::tuple_size<T>::value>
struct TTupleHelper;

template <class T>
struct TTupleHelper<T, 0U>
{
    static void DeserializeItem(T&, TYsonPullParserCursor*) {}
};

template <class T, size_t Size>
struct TTupleHelper
{
    static void DeserializeItem(T& value, TYsonPullParserCursor* cursor)
    {
        TTupleHelper<T, Size - 1U>::DeserializeItem(value, cursor);
        if ((*cursor)->GetType() != EYsonItemType::EndList) {
            Deserialize(std::get<Size - 1U>(value), cursor);
        }
    }
};

template <class T>
void DeserializeTuple(T& value, TYsonPullParserCursor* cursor)
{
    NDetail::MaybeSkipAttributes(cursor);
    EnsureYsonToken("tuple", *cursor, EYsonItemType::BeginList);
    cursor->Next();
    TTupleHelper<T>::DeserializeItem(value, cursor);
    while ((*cursor)->GetType() != EYsonItemType::EndList) {
        cursor->SkipComplexValue();
    }
    cursor->Next();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T, class A>
void Deserialize(std::vector<T, A>& value, NYson::TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeVector(value, cursor);
}

template <class T>
void Deserialize(std::optional<T>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::MaybeSkipAttributes(cursor);
    if ((*cursor)->GetType() == EYsonItemType::EntityValue) {
        value.reset();
        cursor->Next();
    } else {
        if (!value) {
            value.emplace();
        }
        Deserialize(*value, cursor);
    }
}

// Enum.
template <class T>
void Deserialize(T& value, TYsonPullParserCursor* cursor, std::enable_if_t<TEnumTraits<T>::IsEnum, void*>)
{
    NDetail::MaybeSkipAttributes(cursor);
    if constexpr (TEnumTraits<T>::IsBitEnum) {
        switch ((*cursor)->GetType()) {
            case EYsonItemType::BeginList:
                value = T();
                cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
                    EnsureYsonToken("bit enum", *cursor, EYsonItemType::StringValue);
                    value |= ParseEnum<T>((*cursor)->UncheckedAsString());
                    cursor->Next();
                });
                break;
            case EYsonItemType::StringValue:
                value = ParseEnum<T>((*cursor)->UncheckedAsString());
                cursor->Next();
                break;
            default:
                ThrowUnexpectedYsonTokenException(
                    "bit enum",
                    *cursor,
                    {EYsonItemType::BeginList, EYsonItemType::StringValue});
        }
    } else {
        EnsureYsonToken("enum", *cursor, EYsonItemType::StringValue);
        value = ParseEnum<T>((*cursor)->UncheckedAsString());
        cursor->Next();
    }
}

// SmallVector
template <class T, unsigned N>
void Deserialize(SmallVector<T, N>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeVector(value, cursor);
}

template <class F, class S>
void Deserialize(std::pair<F, S>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<F, S>(), void*>)
{
    NDetail::DeserializeTuple(value, cursor);
}

template <class T, size_t N>
void Deserialize(std::array<T, N>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeTuple(value, cursor);
}

template <class... T>
void Deserialize(std::tuple<T...>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T...>(), void*>)
{
    NDetail::DeserializeTuple(value, cursor);
}

// For any associative container.
template <template<typename...> class C, class... T, class K>
void Deserialize(
    C<T...>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<typename NDetail::TRemoveConst<typename C<T...>::value_type>::Type>(), void*>)
{
    NDetail::DeserializeAssociative(value, cursor);
}

template <class E, class T, E Min, E Max>
void Deserialize(TEnumIndexedVector<E, T, Min, Max>& vector, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::MaybeSkipAttributes(cursor);
    vector = {};
    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        auto key = ExtractTo<E>(cursor);
        if (!vector.IsDomainValue(key)) {
            THROW_ERROR_EXCEPTION("Enum value %Qlv is out of supported range",
                key);
        }
        Deserialize(vector[key], cursor);
    });
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTo>
TTo ExtractTo(TYsonPullParserCursor* cursor)
{
    TTo result;
    Deserialize(result, cursor);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
