#include <yt/yt/library/query/misc/udf_cpp_abi.h>
#include <yt/yt/library/query/misc/function_context.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void udf_with_function_context(
    TExpressionContext* /*expressionContext*/,
    NYT::NQueryClient::TFunctionContext* /*functionContext*/,
    TUnversionedValue* result,
    TUnversionedValue* input)
{
    *result = *input;
}
