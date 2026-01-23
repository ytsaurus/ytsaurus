#pragma once
#include <contrib/ydb/core/fq/libs/protos/fq_private.pb.h>

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

namespace NFq {

NActors::IActor* MakeReadRuleCreatorActor(
    NActors::TActorId owner,
    TString queryId,
    NYdb::TDriver ydbDriver,
    const NYql::IPqGateway::TPtr& pqGateway,
    const ::google::protobuf::RepeatedPtrField<Fq::Private::TopicConsumer>& topicConsumers,
    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials // For each topic
);

} // namespace NFq
