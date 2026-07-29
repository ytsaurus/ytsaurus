#pragma once

#include "permission_cache.h"

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryPoolPermissionCache
    : public TPermissionCache
{
public:
    using TPermissionCache::TPermissionCache;

private:
    bool CanCacheError(const TError& error) noexcept override;
};

DEFINE_REFCOUNTED_TYPE(TQueryPoolPermissionCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
