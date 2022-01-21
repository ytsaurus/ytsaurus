#include "replication_card_replica_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardReplicaTypeHandler
    : public TVirtualTypeHandler
{
public:
    explicit TReplicationCardReplicaTypeHandler(TClient* client)
        : Client_(client)
    { }

private:
    TClient* const Client_;


    EObjectType GetSupportedObjectType() override
    {
        return EObjectType::ReplicationCardReplica;
    }

    TYsonString GetObjectYson(TReplicaId replicaId) override
    {
        auto replicationCardId = ReplicationCardIdFromReplicaId(replicaId);

        TGetReplicationCardOptions getCardOptions;
        getCardOptions.IncludeProgress = true;
        getCardOptions.IncludeHistory = true;
        getCardOptions.BypassCache = true;
        auto card = WaitFor(Client_->GetReplicationCard(replicationCardId, getCardOptions))
            .ValueOrThrow();

        const auto* replicaInfo = card->GetReplicaOrThrow(replicaId, replicationCardId);
        return BuildYsonStringFluently()
            .BeginAttributes()
                .Item("id").Value(replicaId)
                .Item("type").Value(EObjectType::ReplicationCardReplica)
                .Do([&] (auto fluent) {
                    Serialize(
                        *replicaInfo,
                        fluent,
                        static_cast<TReplicationCardFetchOptions&>(getCardOptions));
                })
            .EndAttributes()
            .Entity();
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicationCardReplicaTypeHandler(TClient* client)
{
    return New<TReplicationCardReplicaTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
