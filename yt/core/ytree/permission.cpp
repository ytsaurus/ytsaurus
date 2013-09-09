#include "stdafx.h"
#include "permission.h"

#include <core/misc/foreach.h>
#include <core/misc/string.h>
#include <core/misc/error.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

EPermissionSet AllPermissions(0xffff);
EPermissionSet NonePermissions(0x0000);

static Stroka AllPermissionsName("all");

////////////////////////////////////////////////////////////////////////////////

EPermissionSet ParsePermissions(
    const std::vector<Stroka>& items,
    EPermissionSet supportedPermissions)
{
    auto result = NonePermissions;
    FOREACH (const auto& item, items) {
        if (item == AllPermissionsName) {
            return supportedPermissions;
        } else {
            auto permission = ParseEnum<EPermission>(item);
            if (!(supportedPermissions & permission)) {
                THROW_ERROR_EXCEPTION("Permission is not supported: %s", ~item);
            }
            result |= permission;
        }
    }
    return result;
}

std::vector<Stroka> FormatPermissions(
    EPermissionSet permissions)
{
    auto values = EPermission::GetDomainValues();
    std::vector<Stroka> result;
    FOREACH (auto value, values) {
        if (permissions & value) {
            result.push_back(FormatEnum(EPermission(value)));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

