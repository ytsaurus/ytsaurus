#pragma once

#include <stdlib.h>
#include <stdint.h>

typedef enum EValueType
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
} EValueType;

typedef union TUnversionedValueData
{
    int64_t Int64;
    uint64_t Uint64;
    double Double;
    int8_t Boolean;
    char* String;
} TUnversionedValueData;

typedef struct TUnversionedValue
{
    int16_t Id;
    uint8_t Type;
    uint8_t Aggregate;
    int32_t Length;
    TUnversionedValueData Data;
} TUnversionedValue;

typedef struct TExpressionContext TExpressionContext;

char* AllocateBytes(TExpressionContext* context, size_t size);

void ThrowException(const char* error);
