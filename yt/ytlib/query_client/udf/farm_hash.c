#include <yt_udf.h>

uint64_t FarmHash(
    const TUnversionedValue* begin,
    const TUnversionedValue* end);

void farm_hash(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    result->Data.Uint64 = FarmHash(args, args + args_len);
    result->Type = Uint64;
}
