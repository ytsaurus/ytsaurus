#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void ListHasIntersection(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* lhsYsonList,
    TUnversionedValue* rhsYsonList);

extern "C" void list_has_intersection(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* lhsYsonList,
    TUnversionedValue* rhsYsonList)
{
    if (lhsYsonList->Type == EValueType::Null || rhsYsonList->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    ListHasIntersection(context, result, lhsYsonList, rhsYsonList);
}
