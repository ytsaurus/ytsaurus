#include "chaos_lease_type_handler.h"

#include "client_impl.h"
#include "config.h"
#include "type_handler_detail.h"

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

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

class TChaosLeaseTypeHandler
    : public TVirtualTypeHandler
{
public:
    using TVirtualTypeHandler::TVirtualTypeHandler;

private:
    EObjectType GetSupportedObjectType() override
    {
        return EObjectType::ChaosLease;
    }

    TYsonString GetObjectYson(TChaosLeaseId chaosLeaseId) override
    {
        auto channel = Client_->GetChaosChannelByObjectIdOrThrow(chaosLeaseId);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.GetChaosLease();
        req->SetTimeout(Client_->GetNativeConnection()->GetConfig()->DefaultChaosNodeServiceTimeout);
        ToProto(req->mutable_chaos_lease_id(), chaosLeaseId);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return BuildYsonStringFluently()
            .BeginAttributes()
                .Item("id").Value(chaosLeaseId)
                .Item("type").Value(EObjectType::ChaosLease)
                .Item("timeout").Value(FromProto<TDuration>(rsp->timeout()))
                .Item("last_ping_time").Value(FromProto<TInstant>(rsp->last_ping_time()))
            .EndAttributes()
            .Entity();
    }

    std::optional<TObjectId> DoCreateObject(const TCreateObjectOptions& options) override
    {
        const auto& connectionConfig = Client_->GetNativeConnection()->GetConfig();
        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        // TOOD(gryzlov-ad): Use chaos bundle name. Unify different chaos type handlers with some base class
        auto chaosCellId = attributes->Get<TCellId>("chaos_cell_id");
        auto parentId = attributes->FindAndRemove<TChaosLeaseId>("parent_id");
        auto chaosLeaseTiemout = attributes->GetAndRemove<TDuration>(
            "timeout",
            connectionConfig->DefaultChaosLeaseTimeout);

        auto channel = Client_->GetChaosChannelByCellId(chaosCellId);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));

        auto req = proxy.CreateChaosLease();
        req->SetTimeout(options.Timeout.value_or(connectionConfig->DefaultChaosNodeServiceTimeout));
        req->set_timeout(ToProto(chaosLeaseTiemout));
        ToProto(req->mutable_attributes(), *attributes);
        if (parentId) {
            ToProto(req->mutable_parent_id(), *parentId);
        }

        Client_->SetMutationId(req, options);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return FromProto<TChaosLeaseId>(rsp->chaos_lease_id());
    }

    void DoRemoveObject(
        TReplicationCardId chaosLeaseId,
        const TRemoveNodeOptions& options) override
    {
        auto channel = Client_->GetChaosChannelByObjectIdOrThrow(chaosLeaseId);
        auto proxy = TChaosNodeServiceProxy(std::move(channel));
        // TODO(nadya02): Set the correct timeout here.
        proxy.SetDefaultTimeout(NRpc::HugeDoNotUseRpcRequestTimeout);

        auto req = proxy.RemoveChaosLease();
        req->SetTimeout(options.Timeout.value_or(Client_->GetNativeConnection()->GetConfig()->DefaultChaosNodeServiceTimeout));
        Client_->SetMutationId(req, options);
        ToProto(req->mutable_chaos_lease_id(), chaosLeaseId);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateChaosLeaseTypeHandler(TClient* client)
{
    return New<TChaosLeaseTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
