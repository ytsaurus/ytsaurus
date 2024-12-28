#ifndef PROFILING_HELPERS_H_
#error "Direct inclusion of this file is not allowed, include profiling_helpers.h"
// For the sake of sane code completion.
#include "profiling_helpers.h"
#endif

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE std::optional<std::string> GetCurrentProfilingUser()
{
    return GetProfilingUser(NRpc::GetCurrentAuthenticationIdentity());
}

Y_FORCE_INLINE std::optional<std::string> GetProfilingUser(const NRpc::TAuthenticationIdentity& identity)
{
    if (&identity == &NRpc::GetRootAuthenticationIdentity()) {
        return {};
    }
    return identity.UserTag;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
