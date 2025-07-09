#pragma once

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <contrib/ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <contrib/ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <contrib/ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {

constexpr i64 DqPqDefaultFreeSpace = 16_MB;

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqPqWriteActor(
    NPq::NProto::TDqPqTopicSink&& settings,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    IPqGateway::TPtr pqGateway,
    i64 freeSpace = DqPqDefaultFreeSpace);

void RegisterDqPqWriteActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, const IPqGateway::TPtr& pqGateway, const ::NMonitoring::TDynamicCounterPtr& counters = MakeIntrusive<::NMonitoring::TDynamicCounters>());

} // namespace NYql::NDq
