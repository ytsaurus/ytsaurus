#include <yt/yt/ytlib/query_client/udf/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void udf_with_function_context(
    TExpressionContext* /*expressionContext*/,
    NYT::NQueryClient::TFunctionContext* /*functionContext*/,
    TUnversionedValue* result,
    TUnversionedValue* input)
{
    *result = *input;
}
