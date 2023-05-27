#include "hunk_storage_node_proxy.h"

#include "hunk_storage_node.h"
#include "hunk_tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"
#include "tablet_owner_proxy_base.h"

#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

using ::NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class THunkStorageNodeProxy
    : public TTabletOwnerProxyBase
{
private:
    using TBase = TTabletOwnerProxyBase;

public:
    using TBase::TBase;

private:
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo);

    // TODO(aleksandra-zh): inherit that.
    void ValidateRemoval()
    {
        const auto* hunkStorage = GetThisImpl();
        const auto& associatedNodeIds = hunkStorage->AssociatedNodeIds();
        if (!associatedNodeIds.empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove a hunk storage that is being used by nodes %v",
                MakeShrunkFormattableView(associatedNodeIds, TDefaultFormatter(), 10));
        }
    }

    void RemoveSelf(
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override
    {
        ValidateRemoval();

        TBase::RemoveSelf(request, response, context);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::DoListSystemAttributes(descriptors, /*showTabletAttributes*/ true);

        const auto* node = GetThisImpl();
        auto isExternal = node->IsExternal();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadQuorum)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteQuorum)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QuorumRowCount)
            .SetExternal(isExternal)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Tablets)
            .SetExternal(isExternal)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AssociatedNodes)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::Sealed);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        auto* node = GetThisImpl();
        auto* trunkNode = node->GetTrunkNode();
        auto isExternal = node->IsExternal();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        switch (key) {
            case EInternedAttributeKey::ReadQuorum:
                BuildYsonFluently(consumer)
                    .Value(node->GetReadQuorum());
                return true;

            case EInternedAttributeKey::WriteQuorum:
                BuildYsonFluently(consumer)
                    .Value(node->GetWriteQuorum());
                return true;

            case EInternedAttributeKey::Tablets:
                if (isExternal) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .DoListFor(trunkNode->Tablets(), [&] (TFluentList fluent, TTabletBase* tabletBase) {
                        auto* tablet = tabletBase->As<THunkTablet>();
                        auto* cell = tablet->GetCell();
                        auto* node = tabletManager->FindTabletLeaderNode(tablet);
                        fluent
                            .Item().BeginMap()
                                .Item("index").Value(tablet->GetIndex())
                                .Item("state").Value(tablet->GetState())
                                .Item("statistics").Value(New<TSerializableTabletStatistics>(
                                    tablet->GetTabletStatistics(),
                                    chunkManager))
                                .Item("tablet_id").Value(tablet->GetId())
                                .DoIf(cell, [&] (TFluentMap fluent) {
                                    fluent.Item("cell_id").Value(cell->GetId());
                                })
                                .DoIf(node, [&] (TFluentMap fluent) {
                                    fluent.Item("cell_leader_address").Value(node->GetDefaultAddress());
                                })
                                .Item("error_count").Value(tablet->GetTabletErrorCount())
                            .EndMap();
                    });
                return true;

            default:
                break;
        }

        return TBase::DoGetBuiltinAttribute(key, consumer, /*showTabletAttributes*/ true);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* node = GetThisImpl();

        const auto& objectManager = Bootstrap_->GetObjectManager();

        switch (key) {
            case EInternedAttributeKey::AssociatedNodes: {
                const auto& nodeIdsSet = node->AssociatedNodeIds();
                std::vector nodeIds(nodeIdsSet.begin(), nodeIdsSet.end());
                return objectManager->ResolveObjectIdsToPaths(nodeIds)
                    .Apply(BIND([] (const std::vector<TErrorOr<IObjectManager::TVersionedObjectPath>>& pathOrErrors) {
                        return BuildYsonStringFluently()
                            .DoListFor(pathOrErrors, [] (TFluentList fluent, const TErrorOr<IObjectManager::TVersionedObjectPath>& pathOrError) {
                                auto code = pathOrError.GetCode();
                                if (code == NYTree::EErrorCode::ResolveError || code == NTransactionClient::EErrorCode::NoSuchTransaction) {
                                    return;
                                }
                                const auto& path = pathOrError.ValueOrThrow();
                                fluent
                                    .Item()
                                    .Do([&] (auto fluent) {
                                        NChunkServer::SerializeNodePath(fluent.GetConsumer(), path.Path, path.TransactionId);
                                    });
                            });
                    }).AsyncVia(GetCurrentInvoker()));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
        return TBase::DoInvoke(context);
    }

    THunkStorageNode* GetThisImpl()
    {
        return TBase::GetThisImpl()->As<THunkStorageNode>();
    }
};

DEFINE_YPATH_SERVICE_METHOD(THunkStorageNodeProxy, GetMountInfo)
{
    DeclareNonMutating();
    SuppressAccessTracking();

    context->SetRequestInfo();

    ValidateNotExternal();
    ValidateNoTransaction();

    const auto* trunkNode = GetThisImpl();

    ToProto(response->mutable_table_id(), trunkNode->GetId());

    THashSet<TTabletCell*> cells;
    for (const auto* tablet : trunkNode->Tablets()) {
        auto* cell = tablet->GetCell();
        auto* protoTablet = response->add_tablets();
        ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
        protoTablet->set_mount_revision(tablet->Servant().GetMountRevision());
        protoTablet->set_state(ToProto<int>(tablet->GetState()));
        protoTablet->set_in_memory_mode(ToProto<int>(tablet->GetInMemoryMode()));
        if (cell) {
            ToProto(protoTablet->mutable_cell_id(), cell->GetId());
            cells.insert(cell);
        }
    }

    for (const auto* cell : cells) {
        ToProto(response->add_tablet_cells(), cell->GetDescriptor());
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateHunkStorageNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    THunkStorageNode* trunkNode)
{
    return New<THunkStorageNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
