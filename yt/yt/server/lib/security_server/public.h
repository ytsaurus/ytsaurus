#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IResourceLimitsManager)
DECLARE_REFCOUNTED_STRUCT(IUserAccessValidator)

DECLARE_REFCOUNTED_CLASS(TUserAccessValidatorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
