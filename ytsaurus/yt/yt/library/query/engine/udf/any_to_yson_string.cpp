#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void AnyToYsonString(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* any,
    int anyLength);

extern "C" void any_to_yson_string(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* any,
    int anyLength)
{
    AnyToYsonString(context, result, resultLength, any, anyLength);
}
