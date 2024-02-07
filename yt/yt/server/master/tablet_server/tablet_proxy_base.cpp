#include "tablet_proxy_base.h"

#include "tablet_base.h"
#include "tablet_cell.h"
#include "tablet_action.h"
#include "tablet_manager.h"
#include "tablet_owner_base.h"

#include <yt/yt/server/master/chunk_server/chunk_list.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NOrchid;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTabletProxyBase::TTabletProxyBase(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTabletBase* tablet)
    : TBase(bootstrap, metadata, tablet)
{
    RegisterService(
        "orchid",
        BIND(&TTabletProxyBase::CreateOrchidService, Unretained(this)));
}

IYPathServicePtr TTabletProxyBase::CreateOrchidService()
{
    const auto& tabletManager = Bootstrap_->GetTabletManager();

    auto* tablet = GetThisImpl<TTablet>();

    auto* node = tabletManager->FindTabletLeaderNode(tablet);
    if (!node) {
        THROW_ERROR_EXCEPTION("Tablet has no leader node");
    }

    auto cellId = tablet->GetCell()->GetId();

    auto nodeAddresses = node->GetAddressesOrThrow(NNodeTrackerClient::EAddressType::InternalRpc);

    // TODO(max42): make customizable.
    constexpr TDuration timeout = TDuration::Seconds(60);

    return CreateOrchidYPathService(TOrchidOptions{
        .Channel = Bootstrap_->GetNodeChannelFactory()->CreateChannel(nodeAddresses),
        .RemoteRoot = Format("//tablet_cells/%v/%v", cellId, GetOrchidPath(tablet->GetId())),
        .Timeout = timeout,
    });
}

void TTabletProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    const auto* tablet = GetThisImpl();

    descriptors->push_back(EInternedAttributeKey::State);
    descriptors->push_back(EInternedAttributeKey::ExpectedState);
    descriptors->push_back(EInternedAttributeKey::AuxiliaryState);
    descriptors->push_back(EInternedAttributeKey::Statistics);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OwnerPath)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TablePath)
        .SetPresent(tablet->GetType() == EObjectType::Tablet)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MountRevision)
        .SetPresent(tablet->GetCell()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MountTime)
        .SetPresent(tablet->GetCell()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StoresUpdatePrepared)
        .SetPresent(tablet->GetStoresUpdatePreparedTransaction() != nullptr));
    descriptors->push_back(EInternedAttributeKey::Index);
    descriptors->push_back(EInternedAttributeKey::OwnerId);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TableId)
        .SetPresent(tablet->GetType() == EObjectType::Tablet));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkListId)
        .SetPresent(tablet->GetChunkList()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkChunkListId)
        .SetPresent(tablet->GetHunkChunkList()));
    descriptors->push_back(EInternedAttributeKey::InMemoryMode);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellId)
        .SetPresent(tablet->GetCell()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AuxiliaryCellId)
        .SetPresent(tablet->AuxiliaryServant().GetCell()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ActionId)
        .SetPresent(tablet->GetAction()));
    descriptors->push_back(EInternedAttributeKey::ErrorCount);
    descriptors->push_back(EInternedAttributeKey::Servants);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::NodeEndpointId)
        .SetPresent(tablet->GetCell()));
}

bool TTabletProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    auto* tablet = GetThisImpl();
    auto* chunkList = tablet->GetChunkList();
    auto* hunkChunkList = tablet->GetHunkChunkList();
    auto* table = tablet->GetOwner();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    switch (key) {
        case EInternedAttributeKey::State:
            BuildYsonFluently(consumer)
                .Value(tablet->GetState());
            return true;

        case EInternedAttributeKey::ExpectedState:
            BuildYsonFluently(consumer)
                .Value(tablet->GetExpectedState());
            return true;

        case EInternedAttributeKey::AuxiliaryState:
            BuildYsonFluently(consumer)
                .Value(tablet->AuxiliaryServant().GetState());
            return true;

        case EInternedAttributeKey::Statistics:
            BuildYsonFluently(consumer)
                .Value(New<TSerializableTabletStatistics>(
                    tablet->GetTabletStatistics(),
                    chunkManager));
            return true;

        case EInternedAttributeKey::OwnerPath:
        case EInternedAttributeKey::TablePath:
            if (!IsObjectAlive(table) || table->IsForeign()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(cypressManager->GetNodePath(
                    table->GetTrunkNode(),
                    /*transaction*/ nullptr));
            return true;

        case EInternedAttributeKey::MountRevision:
            if (!tablet->GetCell()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(tablet->Servant().GetMountRevision());
            return true;

        case EInternedAttributeKey::MountTime:
            if (!tablet->GetCell()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(tablet->Servant().GetMountTime());
            return true;

        case EInternedAttributeKey::StoresUpdatePreparedTransactionId:
            if (!tablet->GetStoresUpdatePreparedTransaction()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(tablet->GetStoresUpdatePreparedTransaction()->GetId());
            return true;

        case EInternedAttributeKey::Index:
            BuildYsonFluently(consumer)
                .Value(tablet->GetIndex());
            return true;

        case EInternedAttributeKey::OwnerId:
        case EInternedAttributeKey::TableId:
            BuildYsonFluently(consumer)
                .Value(table->GetId());
            return true;

        case EInternedAttributeKey::ChunkListId:
            if (!chunkList) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(chunkList->GetId());
            return true;

        case EInternedAttributeKey::HunkChunkListId:
            if (!hunkChunkList) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(hunkChunkList->GetId());
            return true;

        case EInternedAttributeKey::InMemoryMode:
            BuildYsonFluently(consumer)
                .Value(tablet->GetInMemoryMode());
            return true;

        case EInternedAttributeKey::CellId:
            if (!tablet->GetCell()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(tablet->GetCell()->GetId());
            return true;

        case EInternedAttributeKey::AuxiliaryCellId: {
            auto* cell = tablet->AuxiliaryServant().GetCell();
            if (!cell) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(cell->GetId());
            return true;
        }

        case EInternedAttributeKey::ActionId:
            if (!tablet->GetAction()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(tablet->GetAction()->GetId());
            return true;

        case EInternedAttributeKey::TabletErrorCount:
            BuildYsonFluently(consumer)
                .Value(tablet->GetTabletErrorCount());
            return true;

        case EInternedAttributeKey::Servants: {
            auto onServant = [] (const TTabletServant& servant, TFluentMap fluent) {
                fluent
                    .Item("cell_id").Value(GetObjectId(servant.GetCell()))
                    .Item("state").Value(servant.GetState())
                    .Item("mount_revision").Value(servant.GetMountRevision())
                    .Item("mount_time").Value(servant.GetMountTime())
                    .DoIf(servant.GetMovementRole() != NTabletNode::ESmoothMovementRole::None, [&] (TFluentMap fluent) {
                        fluent
                            .Item("role").Value(servant.GetMovementRole())
                            .Item("stage").Value(servant.GetMovementStage());
                    });
            };
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("main").DoMap(BIND(onServant, tablet->Servant()))
                    .DoIf(tablet->AuxiliaryServant(), [&] (TFluentMap fluent) {
                        fluent.Item("auxiliary").DoMap(BIND(onServant, tablet->AuxiliaryServant()));
                    })
                .EndMap();
            return true;
        }

        case EInternedAttributeKey::NodeEndpointId:
            if (!tablet->GetCell()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(tablet->GetNodeEndpointId());
            return true;

        default:
            break;
    };

    return TBase::GetBuiltinAttribute(key, consumer);
}

TFuture<TYsonString> TTabletProxyBase::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    const auto* tablet = GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::OwnerPath:
        case EInternedAttributeKey::TablePath: {
            auto* table = tablet->GetOwner();
            if (!IsObjectAlive(table)) {
                break;
            }
            return FetchFromShepherd(FromObjectId(table->GetId()) + "/@path");
        }

        default:
            break;
    }

    return TBase::GetBuiltinAttributeAsync(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
