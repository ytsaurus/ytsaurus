#include <yt_udf.h>

void sum_udf2(
    TExecutionContext* context,
    TUnversionedValue* result_value,
    TUnversionedValue* n1,
    TUnversionedValue* ns,
    int ns_len)
{
    int64_t result = n1->Data.Int64;
    for (int i = 0; i < ns_len; i++) {
        result += ns[i].Data.Int64;
    }
    result_value->Type = Int64;
    result_value->Data.Int64 = result;
}
