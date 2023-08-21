#pragma once

#include <yt/yt/client/security_client/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TSerializableAccessControlEntry;
struct TSerializableAccessControlList;
struct TPermissionKey;

DECLARE_REFCOUNTED_CLASS(TPermissionCache)
DECLARE_REFCOUNTED_CLASS(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

