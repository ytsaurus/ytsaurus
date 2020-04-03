#pragma once

#include "public.h"

#include <yp/server/access_control/access_control_manager.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template<class TMany, class TOne>
void ValidateUsePermissionIfChanged(
    const TManyToOneAttribute<TMany, TOne>& attribute,
    const NAccessControl::TAccessControlManagerPtr& accessControlManager)
{
    if (attribute.IsChanged()) {
        accessControlManager->ValidatePermission(attribute.Load(), NAccessControl::EAccessControlPermission::Use);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
