#include "udf_cpp_abi.h"

using namespace NYT::NQueryClient::NUdf;

extern "C" void ListContains(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* ysonList,
    TUnversionedValue* what);

extern "C" void list_contains(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* ysonList,
    TUnversionedValue* what)
{
    if (ysonList->Type == EValueType::Null) {
        ClearValue(result);
        result->Type = EValueType::Null;
        return;
    }

    ListContains(context, result, ysonList, what);
}
