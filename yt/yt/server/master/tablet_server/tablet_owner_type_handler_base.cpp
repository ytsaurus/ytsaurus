#include "tablet_owner_type_handler_base.h"

#include "hunk_storage_node.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/table_server/replicated_table_node.h>

namespace NYT::NTabletServer {

using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

template<class TImpl>
bool TTabletOwnerTypeHandlerBase<TImpl>::IsSupportedInheritableAttribute(const TString& key) const
{
    static const THashSet<TString> SupportedInheritableAttributes{
        "in_memory_mode",
        "tablet_cell_bundle",
    };

    if (SupportedInheritableAttributes.contains(key)) {
        return true;
    }

    return TBase::IsSupportedInheritableAttribute(key);
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoDestroy(TImpl* owner)
{
    if (owner->IsTrunk()) {
        const auto& tabletManager = this->Bootstrap_->GetTabletManager();
        tabletManager->DestroyTabletOwner(owner);
    }

    TBase::DoDestroy(owner);
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    const auto& tabletManager = this->Bootstrap_->GetTabletManager();
    tabletManager->ValidateCloneTabletOwner(
        sourceNode,
        mode,
        account);

    TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

    tabletManager->CloneTabletOwner(sourceNode, clonedTrunkNode, mode);

    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    if (trunkSourceNode->HasCustomTabletOwnerAttributes()) {
        clonedTrunkNode->InitializeCustomTabletOwnerAttributes();
        clonedTrunkNode->GetCustomTabletOwnerAttributes()->CopyFrom(
            trunkSourceNode->GetCustomTabletOwnerAttributes());
    }
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoBeginCopy(
    TImpl* node,
    TBeginCopyContext* context)
{
    TBase::DoBeginCopy(node, context);

    const auto& tabletManager = this->Bootstrap_->GetTabletManager();
    tabletManager->ValidateBeginCopyTabletOwner(node, context->GetMode());

    using NYT::Save;

    auto* trunkNode = node->GetTrunkNode();

    Save(*context, trunkNode->TabletCellBundle());

    Save(*context, trunkNode->HasCustomTabletOwnerAttributes());
    if (trunkNode->HasCustomTabletOwnerAttributes()) {
        trunkNode->GetCustomTabletOwnerAttributes()->BeginCopy(context);
    }
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoEndCopy(
    TImpl* node,
    TEndCopyContext* context,
    ICypressNodeFactory* factory)
{
    TBase::DoEndCopy(node, context, factory);

    const auto& tabletManager = this->Bootstrap_->GetTabletManager();

    using NYT::Load;

    if (auto* bundle = Load<TTabletCellBundle*>(*context)) {
        const auto& objectManager = this->Bootstrap_->GetObjectManager();
        objectManager->ValidateObjectLifeStage(bundle);
        tabletManager->SetTabletCellBundle(node, bundle);
    }

    if (Load<bool>(*context)) {
        node->InitializeCustomTabletOwnerAttributes();
        node->GetCustomTabletOwnerAttributes()->EndCopy(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TTabletOwnerTypeHandlerBase<TTableNode>;
template class TTabletOwnerTypeHandlerBase<TReplicatedTableNode>;
template class TTabletOwnerTypeHandlerBase<THunkStorageNode>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
