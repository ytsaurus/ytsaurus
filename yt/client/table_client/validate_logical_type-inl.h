#pragma once
#ifndef VALIDATE_LOGICAL_TYPE_INL_H_
#error "Direct inclusion of this file is not allowed, include validate_logical_type.h"
// For the sake of sane code completion.
#include "validate_logical_type.h"
#endif

#include "logical_type.h"

#include <yt/core/misc/error.h>

#include <util/charset/utf8.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename TInt>
static inline void ValidateIntegerRange(TInt value)
{
    static_assert(std::is_same_v<TInt, i64> || std::is_same_v<TInt, ui64>);
    static_assert(std::is_integral<T>::value, "type must be integral");
    static_assert(std::is_signed_v<T> == std::is_signed_v<TInt>);

    if (value < Min<T>() || value > Max<T>()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::SchemaViolation,
            "Value %v is out of allowed range [%v, %v]",
            value,
            Min<T>(),
            Max<T>());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(i64 value)
{
    if constexpr (type == ESimpleLogicalValueType::Int8) {
        NDetail::ValidateIntegerRange<i8>(value);
    } else if constexpr (type == ESimpleLogicalValueType::Int16) {
        NDetail::ValidateIntegerRange<i16>(value);
    } else if constexpr (type == ESimpleLogicalValueType::Int32) {
        NDetail::ValidateIntegerRange<i32>(value);
    } else if constexpr (type == ESimpleLogicalValueType::Int64) {
        // do nothing
    } else {
        static_assert(type == ESimpleLogicalValueType::Int64, "Bad logical type");
    }
}

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(ui64 value)
{
    if constexpr (type == ESimpleLogicalValueType::Uint8) {
        NDetail::ValidateIntegerRange<ui8>(value);
    } else if constexpr (type == ESimpleLogicalValueType::Uint16) {
        NDetail::ValidateIntegerRange<ui16>(value);
    } else if constexpr (type == ESimpleLogicalValueType::Uint32) {
        NDetail::ValidateIntegerRange<ui32>(value);
    } else if constexpr (type == ESimpleLogicalValueType::Uint64) {
        // do nothing
    } else {
        static_assert(type == ESimpleLogicalValueType::Uint64, "Bad logical type");
    }
}

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(double /*value*/)
{
    if constexpr (type == ESimpleLogicalValueType::Double)  {
        // do nothing
    } else {
        static_assert(type == ESimpleLogicalValueType::Double, "Bad logical type");
    }
}

template <ESimpleLogicalValueType type>
Y_FORCE_INLINE void ValidateSimpleLogicalType(bool /*value*/)
{
    if constexpr (type == ESimpleLogicalValueType::Boolean)  {
        // do nothing
    } else {
        static_assert(type == ESimpleLogicalValueType::Boolean, "Bad logical type");
    }
}

template <ESimpleLogicalValueType type>
void ValidateSimpleLogicalType(TStringBuf value)
{
    if constexpr (type == ESimpleLogicalValueType::String)  {
        // do nothing
    } else if constexpr (type == ESimpleLogicalValueType::Utf8) {
        if (UTF8Detect(value.data(), value.size()) == NotUTF8) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Not a valid utf8 string");
        }
    } else {
        static_assert(type == ESimpleLogicalValueType::String, "Bad logical type");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
