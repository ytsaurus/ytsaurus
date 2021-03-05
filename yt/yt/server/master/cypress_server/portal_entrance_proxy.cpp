#include "portal_entrance_proxy.h"
#include "portal_entrance_node.h"
#include "node_proxy_detail.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TPortalEntranceProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TPortalEntranceNode>
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    TPortalEntranceProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TPortalEntranceNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TPortalEntranceNode>;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::RemovalStarted);
        descriptors->push_back(EInternedAttributeKey::ExitCellTag);
        descriptors->push_back(EInternedAttributeKey::ExitNodeId);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        switch (key) {
            case EInternedAttributeKey::RemovalStarted:
                BuildYsonFluently(consumer)
                    .Value(node->GetRemovalStarted());
                return true;

            case EInternedAttributeKey::ExitCellTag:
                BuildYsonFluently(consumer)
                    .Value(node->GetExitCellTag());
                return true;

            case EInternedAttributeKey::ExitNodeId:
                BuildYsonFluently(consumer)
                    .Value(MakePortalExitNodeId(node->GetId(), node->GetExitCellTag()));
                return true;

            case EInternedAttributeKey::Opaque:
                YT_ASSERT(node->GetOpaque());
                // Let the base class handle it.
                break;

            case EInternedAttributeKey::RecursiveResourceUsage:
                // NB: suppress falling back to base class, forcing async getter to be called.
                return false;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto* node = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::RecursiveResourceUsage: {
                auto exitCellTag = node->GetExitCellTag();
                auto portalExitNodeId = MakePortalExitNodeId(node->GetId(), exitCellTag);

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                auto channel = multicellManager->GetMasterChannelOrThrow(exitCellTag, NHydra::EPeerKind::Follower);

                TObjectServiceProxy proxy(channel);
                auto batchReq = proxy.ExecuteBatch();

                auto req = TYPathProxy::Get(FromObjectId(portalExitNodeId) + "/@" + key.Unintern());
                batchReq->AddRequest(req);

                return batchReq->Invoke()
                    .Apply(BIND([=, this_ = MakeStrong(this)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                        auto cumulativeError = GetCumulativeError(batchRspOrError);
                        THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError, "Error fetching attribute %Qv of portal exit %v from cell %v",
                            key.Unintern(),
                            portalExitNodeId,
                            exitCellTag);

                        const auto& batchRsp = batchRspOrError.Value();
                        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                        const auto& rsp = rspOrError.Value();

                        auto serializableResourceUsage = ConvertTo<TSerializableClusterResourcesPtr>(TYsonString(rsp->value()));
                        const auto& chunkManager = Bootstrap_->GetChunkManager();
                        auto resourceUsage = serializableResourceUsage->ToClusterResources(chunkManager);

                        // NB: account for both portal entrance and portal exit resource usage.
                        resourceUsage += node->GetTotalResourceUsage();

                        auto resourceSerializer = New<TSerializableClusterResources>(chunkManager, resourceUsage);
                        return BuildYsonStringFluently()
                            .Value(resourceSerializer);
                    }).AsyncVia(GetCurrentInvoker()));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        switch (key) {
            case EInternedAttributeKey::Opaque: {
                auto opaque = ConvertTo<bool>(value);
                if (!opaque) {
                    THROW_ERROR_EXCEPTION("Portal entrances cannot be made non-opaque");
                }
                return true;

            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreatePortalEntranceProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TPortalEntranceNode* trunkNode)
{
    return New<TPortalEntranceProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
