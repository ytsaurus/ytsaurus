#pragma once

#include <contrib/ydb/core/grpc_services/base/base.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <memory>

namespace NKikimr::NSqsTopic::V1 {
    std::unique_ptr<NActors::IActor> CreateListQueuesActor(NKikimr::NGRpcService::IRequestOpCtx* msg);
} // namespace NKikimr::NSqsTopic::V1
