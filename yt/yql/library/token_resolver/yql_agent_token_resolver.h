#pragma once

#include <yt/yql/library/token_resolver/proto/config.pb.h>
#include <yt/yql/providers/yt/lib/yt_token_resolver/yt_token_resolver.h>

namespace NYql {

IYtTokenResolver::TPtr CreateYqlAgentTokenResolver(const TYqlAgentTokenResolverConfig& config);

} // namespace NYql
