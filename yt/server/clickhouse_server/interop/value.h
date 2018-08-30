#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <vector>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

enum class EValueType
{
    /// Represents NULL marker.
    Null = 0,

    /// Signed integer value.
    Int,

    /// Unsigned integer value.
    UInt,

    /// Floating point value.
    Float,

    /// Boolean value.
    Boolean,

    /// String value.
    String,
};

////////////////////////////////////////////////////////////////////////////////

struct TValue
{
    /// Value type.
    EValueType Type;

    /// Length of a variable-size value (String).
    ui32 Length;

    union {
        /// Signed integer value.
        i64 Int;

        /// Unsigned integer value.
        ui64 UInt;

        /// Floating point value.
        double Float;

        /// Boolean value.
        bool Boolean;

        /// String value (not owning).
        const char* String;
    };

    void SetNull()
    {
        Type = EValueType::Null;
        Length = 0;
    }

    void SetInt(i64 value)
    {
        Type = EValueType::Int;
        Length = 0;
        Int = value;
    }

    void SetUInt(ui64 value)
    {
        Type = EValueType::UInt;
        Length = 0;
        UInt = value;
    }

    void SetFloat(double value)
    {
        Type = EValueType::Float;
        Length = 0;
        Float = value;
    }

    void SetBoolean(bool value)
    {
        Type = EValueType::Boolean;
        Length = 0;
        Boolean = value;
    }

    void SetString(const char* value, size_t len)
    {
        Type = EValueType::String;
        Length = len;
        String = value;
    }

#define DEFINE_IS_METHOD(type) \
    bool Is##type() const \
    { \
        return Type == EValueType::type; \
    }

    DEFINE_IS_METHOD(Int)
    DEFINE_IS_METHOD(UInt)
    DEFINE_IS_METHOD(Float)
    DEFINE_IS_METHOD(Boolean)
    DEFINE_IS_METHOD(String)

#undef DEFINE_IS_METHOD

    TStringBuf AsStringBuf() const
    {
        if (Type != EValueType::String) {
            ythrow yexception() << "Expected String, found " << Type;
        }
        return TStringBuf(String, Length);
    }
};

static_assert(sizeof(TValue) == 16, "sizeof(TValue) == 16");

using TRow = std::vector<TValue>;

}   // namespace NInterop
