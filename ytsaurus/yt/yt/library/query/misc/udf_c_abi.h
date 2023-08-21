#pragma once

#include <stdlib.h>
#include <stdint.h>

////////////////////////////////////////////////////////////////////////////////

typedef enum EValueType
{
    VT_Min = 0x00,
    VT_TheBottom = 0x01,
    VT_Null = 0x02,
    VT_Int64 = 0x03,
    VT_Uint64 = 0x04,
    VT_Double = 0x05,
    VT_Boolean = 0x06,
    VT_String = 0x10,
    VT_Any = 0x11,
    VT_Max = 0xef
} EValueType;

typedef union TUnversionedValueData
{
    int64_t Int64;
    uint64_t Uint64;
    double Double;
    int8_t Boolean;
    char* String;
} TUnversionedValueData;

typedef enum EValueFlags
{
    VF_None = 0x00,
    VF_Aggregate = 0x01
} EValueFlags;

typedef struct TUnversionedValue
{
    uint16_t Id;
    uint8_t Type;
    uint8_t Flags;
    uint32_t Length;
    TUnversionedValueData Data;
} TUnversionedValue;

typedef struct TExpressionContext TExpressionContext;

//! Allocates #size bytes within #context.
char* AllocateBytes(TExpressionContext* context, size_t size);

//! Throws exception with #error message.
void ThrowException(const char* error);

//! Fully initializes #value but leaves it in invalid state.
void ClearValue(TUnversionedValue* value);

////////////////////////////////////////////////////////////////////////////////

#define UDF_C_ABI_INL_H_
#include "udf_c_abi-inl.h"
#undef UDF_C_ABI_INL_H_
