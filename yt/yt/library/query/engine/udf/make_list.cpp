#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void MakeList(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount);

extern "C" void make_list(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount)
{
    MakeList(context, result, args, argCount);
}
