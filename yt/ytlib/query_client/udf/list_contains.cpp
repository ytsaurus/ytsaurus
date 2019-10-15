#include "yt_udf_cpp.h"

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
        result->Type = EValueType::Null;
        return;
    }

    ListContains(context, result, ysonList, what);
}
