#include "yt_token_resolver.h"

#ifdef _linux_
#include "yql_agent_token_resolver.h"
#endif

namespace NYql {

IYtTokenResolver::TPtr CreateYtTokenResolver(const TYtTokenResolverConfig& config) {
#ifdef _linux_
    return config.HasYqlAgent() ? CreateYqlAgentTokenResolver(config.GetYqlAgent()) : nullptr;
#else
    Y_UNUSED(config);
    return nullptr;
#endif
}

} // namespace NYql
