#include <stdlib.h>
#include <stdbool.h>

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
    long Int64;
    unsigned long Uint64;
    double Double;
    bool Boolean;
    const char* String;
} TUnversionedValueData;

typedef struct TUnversionedValue
{
    short Id;
    EValueType Type;
    int Length;

    TUnversionedValueData Data;
} TUnversionedValue;

void is_null_udf(TUnversionedValue* result, TUnversionedValue* value)
{
    result->Type = Boolean;
    bool isnull = value == NULL;
    result->Data.Boolean = isnull;
}
