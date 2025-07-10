#pragma once

#include <contrib/ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>
#include <util/generic/ptr.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <contrib/ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

namespace NFq::NRowDispatcher {

struct IActorFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IActorFactory>;

    virtual NActors::TActorId RegisterTopicSession(
        const TString& readGroup,
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        NActors::TActorId compileServiceActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NMonitoring::TDynamicCounterPtr& countersRoot,
        const NYql::IPqGateway::TPtr& pqGateway,
        ui64 maxBufferSize) const = 0;
};

IActorFactory::TPtr CreateActorFactory();

}
