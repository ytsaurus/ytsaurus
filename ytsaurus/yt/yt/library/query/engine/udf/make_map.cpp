#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void MakeMap(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount);

extern "C" void make_map(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount)
{
    MakeMap(context, result, args, argCount);
}
