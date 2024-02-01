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

class TReplicationCardCollocationTypeHandler
    : public TVirtualTypeHandler
{
public:
    using TVirtualTypeHandler::TVirtualTypeHandler;

private:
    EObjectType GetSupportedObjectType() override
    {
        return EObjectType::ReplicationCardCollocation;
    }

    std::optional<TObjectId> DoCreateObject(const TCreateObjectOptions& options) override
    {
        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        auto chaosCellId = attributes->Get<TCellId>("chaos_cell_id", TCellId());
        auto tablePaths = attributes->Get<std::vector<TYPath>>("table_paths");
        auto type = attributes->Get<NTableClient::ETableCollocationType>("type");

        if (type != ETableCollocationType::Replication) {
            THROW_ERROR_EXCEPTION("Unknown collocation type: %v",
                type);
        }

        if (tablePaths.empty()) {
            THROW_ERROR_EXCEPTION("Cannot create empty replication card collocation");
        }

        if (!chaosCellId) {
            chaosCellId = GetChaosCellId(tablePaths[0]);
        }

        auto replicationCardIds = GetReplicationCardIds(tablePaths);
        auto channel = Client_->GetChaosChannelByCellId(chaosCellId);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.CreateReplicationCardCollocation();
        Client_->SetMutationId(req, options);
        ToProto(req->mutable_replication_card_ids(), replicationCardIds);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return FromProto<TReplicationCardCollocationId>(rsp->replication_card_collocation_id());
    }

    std::vector<TReplicationCardId> GetReplicationCardIds(const std::vector<TYPath>& tablePaths)
    {
        TGetNodeOptions options;
        options.Attributes = {"type", "replication_card_id"};
        std::vector<TFuture<TYsonString>> futureYsons;

        for (const auto& tablePath : tablePaths) {
            futureYsons.push_back(Client_->GetNode(tablePath + "/@", options));
        }

        auto ysons = WaitFor(AllSucceeded(futureYsons))
            .ValueOrThrow();

        std::vector<TReplicationCardId> replicationCardIds;
        for (int index = 0; index < std::ssize(ysons); ++index) {
            const auto& yson = ysons[index];
            auto attributes = ConvertToAttributes(yson);
            auto type = attributes->Get<EObjectType>("type");
            if (type != EObjectType::ChaosReplicatedTable) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    tablePaths[index],
                    EObjectType::ChaosReplicatedTable,
                    type);
            }

            replicationCardIds.push_back(attributes->Get<TReplicationCardId>("replication_card_id"));
        }

        return replicationCardIds;
    }

    TString GetChaosCellBundle(TYPath path)
    {
        auto yson = WaitFor(Client_->GetNode(Format("%v/@chaos_cell_bundle", path), TGetNodeOptions{}))
            .ValueOrThrow();
        return ConvertTo<TString>(yson);
    }

    TCellId GetChaosCellId(TYPath path)
    {
        auto yson = WaitFor(Client_->GetNode(Format("//sys/chaos_cell_bundles/%v/@metadata_cell_id", GetChaosCellBundle(path)), TGetNodeOptions{}))
            .ValueOrThrow();
        return ConvertTo<TCellId>(yson);
    }

    TYsonString GetObjectYson(TReplicationCardCollocationId /*replicationCardCollocationId*/) override
    {
        THROW_ERROR_EXCEPTION("Method is not implemented");
    }

    void DoRemoveObject(
        TReplicationCardCollocationId /*replicationCardCollocationId*/,
        const TRemoveNodeOptions& /*options*/) override
    {
        THROW_ERROR_EXCEPTION("Method is not implemented");
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicationCardCollocationTypeHandler(TClient* client)
{
    return New<TReplicationCardCollocationTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
