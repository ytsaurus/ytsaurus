#include "query_pool_permission_cache.h"

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

bool TQueryPoolPermissionCache::CanCacheError(const TError& error) noexcept
{
    return TPermissionCache::CanCacheError(error) ||
        error.FindMatching(NYTree::EErrorCode::ResolveError).has_value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
