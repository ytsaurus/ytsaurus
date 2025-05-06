#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <contrib/ydb/library/yql/providers/generic/proto/source.pb.h>
#include <contrib/ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NYql::NDq {

    std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*>
    CreateGenericReadActor(
        NConnector::IClient::TPtr genericClient,
        Generic::TSource&& params,
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const THashMap<TString, TString>& secureParams,
        ui64 taskId,
        const THashMap<TString, TString>& taskParams,
        const TVector<TString>& readRanges,
        const NActors::TActorId& computeActorId,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory);

} // namespace NYql::NDq
