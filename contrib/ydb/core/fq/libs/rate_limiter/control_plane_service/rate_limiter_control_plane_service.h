#pragma once
#include <contrib/ydb/core/fq/libs/config/protos/rate_limiter.pb.h>
#include <contrib/ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <contrib/ydb/library/security/ydb_credentials_provider_factory.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NFq {

NActors::IActor* CreateRateLimiterControlPlaneService(
    const NConfig::TRateLimiterConfig& rateLimiterConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory);

} // namespace NFq
