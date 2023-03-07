#pragma once

#include <yt/client/security_client/public.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TSerializableAccessControlEntry;
struct TSerializableAccessControlList;

struct TPermissionKey;
DECLARE_REFCOUNTED_CLASS(TPermissionCache)
DECLARE_REFCOUNTED_STRUCT(TPermissionCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

