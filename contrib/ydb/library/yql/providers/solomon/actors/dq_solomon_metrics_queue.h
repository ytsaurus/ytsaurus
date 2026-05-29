#pragma once

#include "dq_solomon_actors_util.h"

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/yql/providers/solomon/common/util.h>

namespace NYql::NDq {

NActors::IActor* CreateSolomonMetricsQueueActor(
    ui64 consumersCount,
    TDqSolomonReadParams readParams,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
    const NSo::TSolomonReadActorConfig& cfg);

} // namespace NYql::NDq
