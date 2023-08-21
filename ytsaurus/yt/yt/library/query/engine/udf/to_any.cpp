#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void ToAny(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value);

extern "C" void to_any(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    ToAny(context, result, value);
}
