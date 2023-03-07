#include "yt_udf_cpp.h"

extern "C" void HasPermissions(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* ysonAcl,
    TUnversionedValue* ysonSubjectList,
    TUnversionedValue* ysonPermissionList);

extern "C" void has_permissions(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* ysonAcl,
    TUnversionedValue* ysonSubjectList,
    TUnversionedValue* ysonPermissionList)
{
    HasPermissions(context, result, ysonAcl, ysonSubjectList, ysonPermissionList);
}
