#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void Split(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* string,
    TUnversionedValue* delimeter);

extern "C" void split(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* string,
    TUnversionedValue* delimeter)
{
    Split(context, result, string, delimeter);
}
