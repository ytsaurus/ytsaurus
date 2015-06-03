#include <core/misc/hyperloglog.h>

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
    int16_t Type;
    int32_t Length;
    TUnversionedValueData Data;
} TUnversionedValue;

typedef struct TExecutionContext TExecutionContext;

extern "C" char* AllocatePermanentBytes(TExecutionContext* context, size_t size);

extern "C" void cardinality_init(
    TExecutionContext* context,
    TUnversionedValue* result)
{
    auto hll = AllocatePermanentBytes(context, sizeof(NYT::HyperLogLog<14>));
    new (hll) NYT::HyperLogLog<14>();

    result->Type = EValueType::String;
    result->Length = sizeof(NYT::HyperLogLog<14>);
    result->Data.String = hll;
}

extern "C" void cardinality_update(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = EValueType::String;
    result->Length = sizeof(NYT::HyperLogLog<14>);
    result->Data.String = state->Data.String;

    auto hll = reinterpret_cast<NYT::HyperLogLog<14>*>(state->Data.String);

    if (newValue->Type == String) {
        hll->Add(newValue->Data.String, newValue->Length);
    } else if (newValue->Type == Uint64) {
        hll->Add(newValue->Data.Uint64);
    } else if (newValue->Type == Int64) {
        hll->Add(newValue->Data.Int64);
    } else if (newValue->Type == Double) {
        hll->Add(newValue->Data.Double);
    } else { /* newValue->Type == Boolean */
        hll->Add(newValue->Data.Boolean);
    }
}

extern "C" void cardinality_merge(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    result->Type = EValueType::String;
    result->Length = sizeof(NYT::HyperLogLog<14>);
    result->Data.String = state1->Data.String;

    auto hll1 = reinterpret_cast<NYT::HyperLogLog<14>*>(state1->Data.String);
    auto hll2 = reinterpret_cast<NYT::HyperLogLog<14>*>(state2->Data.String);
    hll1->Merge(hll2);
}

extern "C" void cardinality_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    auto hll = reinterpret_cast<NYT::HyperLogLog<14>*>(state->Data.String);
    result->Type = EValueType::Uint64;
    auto card = hll->EstimateCardinality();
    result->Data.Uint64 = card;
}
