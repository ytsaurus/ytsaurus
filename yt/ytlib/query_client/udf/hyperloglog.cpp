#include <core/misc/hyperloglog.h>
#include <yt_udf_cpp.h>

extern "C" void cardinality_init(
    TExecutionContext* context,
    TUnversionedValue* result)
{
    auto hll = AllocatePermanentBytes(context, sizeof(NYT::THyperLogLog<14>));
    new (hll) NYT::THyperLogLog<14>();

    result->Type = EValueType::String;
    result->Length = sizeof(NYT::THyperLogLog<14>);
    result->Data.String = hll;
}

extern "C" void cardinality_update(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    result->Type = EValueType::String;
    result->Length = sizeof(NYT::THyperLogLog<14>);
    result->Data.String = state->Data.String;

    auto hll = reinterpret_cast<NYT::THyperLogLog<14>*>(state->Data.String);

    switch (newValue->Type) {
        case String:
            hll->Add(newValue->Data.String, newValue->Length);
            break;
        case Uint64:
            hll->Add(newValue->Data.Uint64);
            break;
        case Int64:
            hll->Add(newValue->Data.Int64);
            break;
        case Double:
            hll->Add(newValue->Data.Double);
            break;
        default: /* Boolean */
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
    result->Length = sizeof(NYT::THyperLogLog<14>);
    result->Data.String = state1->Data.String;

    auto hll1 = reinterpret_cast<NYT::THyperLogLog<14>*>(state1->Data.String);
    auto hll2 = reinterpret_cast<NYT::THyperLogLog<14>*>(state2->Data.String);
    hll1->Merge(*hll2);
}

extern "C" void cardinality_finalize(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    auto hll = reinterpret_cast<NYT::THyperLogLog<14>*>(state->Data.String);
    result->Type = EValueType::Uint64;
    auto card = hll->EstimateCardinality();
    result->Data.Uint64 = card;
}
