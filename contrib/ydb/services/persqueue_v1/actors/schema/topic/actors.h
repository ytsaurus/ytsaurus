#pragma once

#include <contrib/ydb/core/grpc_services/base/base.h>
#include <contrib/ydb/services/persqueue_v1/actors/events.h>

namespace NKikimr::NGRpcProxy::V1::NTopic {

NActors::IActor* CreateAlterTopicActor(NGRpcService::IRequestOpCtx* request);
NActors::IActor* CreateDropTopicActor(NGRpcService::IRequestOpCtx* request);

} // namespace NKikimr::NGRpcProxy::V1::NTopic
