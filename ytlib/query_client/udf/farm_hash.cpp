#include "yt_udf_cpp.h"

extern "C" uint64_t GetFarmFingerprint(const TUnversionedValue* begin, const TUnversionedValue* end);

extern "C" void farm_hash(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    result->Data.Uint64 = GetFarmFingerprint(args, args + args_len);
    result->Type = EValueType::Uint64;
}
