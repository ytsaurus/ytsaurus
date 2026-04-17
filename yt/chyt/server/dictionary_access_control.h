#pragma once

#include "private.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/ytlib/security_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class IDictionaryAccessControl
    : public virtual TRefCounted
{
public:
    virtual ~IDictionaryAccessControl() = default;

    virtual void Start(DB::ContextMutablePtr serverContext) = 0;

    //! Synchronize user grants for clique dictionaries with the ACLs of all relevant YT sources.
    virtual void SyncUserAccessRights(const std::string& userName) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDictionaryAccessControl)

////////////////////////////////////////////////////////////////////////////////

IDictionaryAccessControlPtr CreateDictionaryAccessControl(
    NSecurityClient::TPermissionCachePtr permissionCache,
    TDictionaryAccessControlConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
