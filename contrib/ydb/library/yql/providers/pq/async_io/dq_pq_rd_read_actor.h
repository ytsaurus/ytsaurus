#pragma once

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <contrib/ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <contrib/ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <contrib/ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>
#include <contrib/ydb/library/security/ydb_credentials_provider_factory.h>

namespace NYql::NDq {
class TDqAsyncIoFactory;

const i64 PQRdReadDefaultFreeSpace = 256_MB;

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqRdReadActor(
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NActors::TActorId& computeActorId,
    const NActors::TActorId& localRowDispatcherActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    i64 bufferSize,
    const IPqGateway::TPtr& pqGateway);

} // namespace NYql::NDq
