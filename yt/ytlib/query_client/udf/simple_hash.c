#include <yt_udf.h>

uint64_t SimpleHash(
    const TUnversionedValue* begin,
    const TUnversionedValue* end);

void simple_hash(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    result->Data.Uint64 = SimpleHash(args, args + args_len);;
    result->Type = Uint64;
}
