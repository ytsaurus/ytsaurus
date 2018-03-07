#pragma once

#include <yt/ytlib/query_client/function_context.h>

#include <stdlib.h>

namespace NYT {

enum class EValueType : uint8_t
{
    Min = 0x00,
    TheBottom = 0x01,
    Null = 0x02,
    Int64 = 0x03,
    Uint64 = 0x04,
    Double = 0x05,
    Boolean = 0x06,
    String = 0x10,
    Any = 0x11,
    Max = 0xef
};

union TUnversionedValueData
{
    int64_t Int64;
    uint64_t Uint64;
    double Double;
    int8_t Boolean;
    char* String;
};

} // namespace NYT

using NYT::EValueType;
using NYT::TUnversionedValueData;

struct TUnversionedValue
{
    int16_t Id;
    EValueType Type;
    uint8_t Aggregate;
    int32_t Length;
    TUnversionedValueData Data;
};

struct TExpressionContext;

extern "C" char* AllocateBytes(TExpressionContext* context, size_t size);

extern "C" void ThrowException(const char* error);

