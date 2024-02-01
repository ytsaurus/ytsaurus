#include "replication_card_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NTableClient;
using namespace NHydra;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardTypeHandler
    : public TVirtualTypeHandler
{
public:
    using TVirtualTypeHandler::TVirtualTypeHandler;

private:
    EObjectType GetSupportedObjectType() override
    {
        return EObjectType::ReplicationCard;
    }

    TYsonString GetObjectYson(TReplicationCardId replicationCardId) override
    {
        TGetReplicationCardOptions getCardOptions;
        getCardOptions.IncludeCoordinators = true;
        getCardOptions.IncludeProgress = true;
        getCardOptions.IncludeHistory = true;
        getCardOptions.BypassCache = true;
        auto card = WaitFor(Client_->GetReplicationCard(replicationCardId, getCardOptions))
            .ValueOrThrow();

        return BuildYsonStringFluently()
            .BeginAttributes()
                .Item("id").Value(replicationCardId)
                .Item("type").Value(EObjectType::ReplicationCard)
                .Do([&] (auto fluent) {
                    Serialize(
                        *card,
                        fluent,
                        static_cast<TReplicationCardFetchOptions&>(getCardOptions));
                })
            .EndAttributes()
            .Entity();
    }

    std::optional<TObjectId> DoCreateObject(const TCreateObjectOptions& options) override
    {
        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        auto chaosCellId = attributes->Get<TCellId>("chaos_cell_id");

        auto channel = Client_->GetChaosChannelByCellId(chaosCellId);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.CreateReplicationCard();
        Client_->SetMutationId(req, options);
        ToProto(req->mutable_table_id(), attributes->Get<TTableId>("table_id", {}));
        req->set_table_path(attributes->Get<TYPath>("table_path", {}));
        req->set_table_cluster_name(attributes->Get<TString>("table_cluster_name", {}));

        // NB(ponasenko-rs): For testing purposes only.
        req->set_bypass_suspended(attributes->Get<bool>("bypass_suspended", false));

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return FromProto<TReplicationCardId>(rsp->replication_card_id());
    }

    void DoRemoveObject(
        TReplicationCardId replicationCardId,
        const TRemoveNodeOptions& options) override
    {
        auto channel = Client_->GetChaosChannelByCardId(replicationCardId, EPeerKind::Leader);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.RemoveReplicationCard();
        Client_->SetMutationId(req, options);
        ToProto(req->mutable_replication_card_id(), replicationCardId);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicationCardTypeHandler(TClient* client)
{
    return New<TReplicationCardTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
