#include <yt/ytlib/query_client/udf/yt_udf_cpp.h>

extern "C" void udf_with_function_context(
    TExpressionContext* expressionContext,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* input)
{
    *result = *input;
}
