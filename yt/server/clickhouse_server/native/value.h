#pragma once

#include <yt/core/misc/common.h>

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <vector>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClickHouseValueType,
    ((Null)    (0))
    ((Int)     (1))
    ((UInt)    (2))
    ((Float)   (3))
    ((Boolean) (4))
    ((String)  (5))
);

////////////////////////////////////////////////////////////////////////////////

struct TValue
{
    /// Value type.
    EClickHouseValueType Type;

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
        Type = EClickHouseValueType::Null;
        Length = 0;
    }

    void SetInt(i64 value)
    {
        Type = EClickHouseValueType::Int;
        Length = 0;
        Int = value;
    }

    void SetUInt(ui64 value)
    {
        Type = EClickHouseValueType::UInt;
        Length = 0;
        UInt = value;
    }

    void SetFloat(double value)
    {
        Type = EClickHouseValueType::Float;
        Length = 0;
        Float = value;
    }

    void SetBoolean(bool value)
    {
        Type = EClickHouseValueType::Boolean;
        Length = 0;
        Boolean = value;
    }

    void SetString(const char* value, size_t len)
    {
        Type = EClickHouseValueType::String;
        Length = len;
        String = value;
    }

    bool IsInt() const
    {
        return (int)Type == (int)decltype(Type)::Int;
    }

#define DEFINE_IS_METHOD(type) \
    bool Is##type() const \
    { \
        return Type == EClickHouseValueType::type; \
    }

    //DEFINE_IS_METHOD(Int)
    DEFINE_IS_METHOD(UInt)
    DEFINE_IS_METHOD(Float)
    DEFINE_IS_METHOD(Boolean)
    DEFINE_IS_METHOD(String)

#undef DEFINE_IS_METHOD

    TStringBuf AsStringBuf() const
    {
        if (Type != EClickHouseValueType::String) {
            ythrow yexception() << "Expected String, found " << Type;
        }
        return TStringBuf(String, Length);
    }
};

static_assert(sizeof(TValue) == 16, "sizeof(TValue) == 16");

using TRow = std::vector<TValue>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
