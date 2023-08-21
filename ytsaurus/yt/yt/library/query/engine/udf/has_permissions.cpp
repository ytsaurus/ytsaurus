#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

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
