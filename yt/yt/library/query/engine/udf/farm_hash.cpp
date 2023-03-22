#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" uint64_t GetFarmFingerprint(
    const TUnversionedValue* begin,
    const TUnversionedValue* end);

extern "C" void farm_hash(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    result->Type = EValueType::Uint64;
    result->Data.Uint64 = GetFarmFingerprint(args, args + args_len);
}
