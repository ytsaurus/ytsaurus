#include "permission_checker.h"

namespace NYT::NSecurityServer {

using namespace NLogging;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPermissionCheckResponse MakeFastCheckPermissionResponse(
    ESecurityAction action,
    const TPermissionCheckBasicOptions& options)
{
    TPermissionCheckResponse response;
    response.Action = action;
    if (options.Columns) {
        response.Columns = std::vector<TPermissionCheckResult>(options.Columns->size());
        for (size_t index = 0; index < options.Columns->size(); ++index) {
            (*response.Columns)[index].Action = action;
        }
    }
    return response;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! "Extend" permission in such way that when any read is requested (read or full_read),
//! we pretend that user requested `EPermission::Read | EPermission::FullRead`.
EPermissionSet ExtendReadPermission(EPermissionSet original)
{
    auto anyhowRead = EPermission::Read | EPermission::FullRead;
    if (Any(original & anyhowRead)) {
        original = original | anyhowRead;
    }
    return original;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
