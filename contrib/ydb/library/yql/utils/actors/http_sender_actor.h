#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/http/http_proxy.h>
#include <library/cpp/retry/retry_policy.h>

namespace NYql::NDq {
    using THttpSenderRetryPolicy = IRetryPolicy<const NHttp::TEvHttpProxy::TEvHttpIncomingResponse*>;

    NActors::IActor* CreateHttpSenderActor(
        NActors::TActorId senderId,
        NActors::TActorId httpProxyId,
        const THttpSenderRetryPolicy::TPtr& retryPolicy);
} // NYql::NDq
