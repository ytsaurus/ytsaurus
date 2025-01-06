#include "tablet_owner_type_handler_base.h"

#include "hunk_storage_node.h"
#include "tablet_manager.h"

#include <yt/yt/server/master/table_server/replicated_table_node.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NTabletServer {

using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

template<class TImpl>
bool TTabletOwnerTypeHandlerBase<TImpl>::IsSupportedInheritableAttribute(const std::string& key) const
{
    static const THashSet<TString> SupportedInheritableAttributes{
        EInternedAttributeKey::InMemoryMode.Unintern(),
        EInternedAttributeKey::TabletCellBundle.Unintern(),
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
        const auto& tabletManager = this->GetBootstrap()->GetTabletManager();
        tabletManager->DestroyTabletOwner(owner);
    }

    TBase::DoDestroy(owner);
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    IAttributeDictionary* inheritedAttributes,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    const auto& tabletManager = this->GetBootstrap()->GetTabletManager();
    tabletManager->ValidateCloneTabletOwner(
        sourceNode,
        mode,
        account);

    TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    tabletManager->CloneTabletOwner(sourceNode, clonedTrunkNode, mode);

    auto* trunkSourceNode = sourceNode->GetTrunkNode();
    if (trunkSourceNode->HasCustomTabletOwnerAttributes()) {
        clonedTrunkNode->InitializeCustomTabletOwnerAttributes();
        clonedTrunkNode->GetCustomTabletOwnerAttributes()->CopyFrom(
            trunkSourceNode->GetCustomTabletOwnerAttributes());
    }
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoSerializeNode(
    TImpl* node,
    TSerializeNodeContext* context)
{
    TBase::DoSerializeNode(node, context);

    const auto& tabletManager = this->GetBootstrap()->GetTabletManager();
    tabletManager->ValidateSerializeTabletOwner(node, context->GetMode());

    using NYT::Save;

    auto* trunkNode = node->GetTrunkNode();

    Save(*context, trunkNode->TabletCellBundle());

    Save(*context, trunkNode->HasCustomTabletOwnerAttributes());
    if (trunkNode->HasCustomTabletOwnerAttributes()) {
        trunkNode->GetCustomTabletOwnerAttributes()->SerializeNode(context);
    }
}

template <class TImpl>
void TTabletOwnerTypeHandlerBase<TImpl>::DoMaterializeNode(
    TImpl* node,
    TMaterializeNodeContext* context)
{
    TBase::DoMaterializeNode(node, context);

    const auto& tabletManager = this->GetBootstrap()->GetTabletManager();

    using NYT::Load;

    if (auto* bundle = Load<TTabletCellBundle*>(*context)) {
        const auto& objectManager = this->GetBootstrap()->GetObjectManager();
        objectManager->ValidateObjectLifeStage(bundle);
        tabletManager->SetTabletCellBundle(node, bundle);
    }

    if (Load<bool>(*context)) {
        node->InitializeCustomTabletOwnerAttributes();
        node->GetCustomTabletOwnerAttributes()->MaterializeNode(context);
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TTabletOwnerTypeHandlerBase<TTableNode>;
template class TTabletOwnerTypeHandlerBase<TReplicatedTableNode>;
template class TTabletOwnerTypeHandlerBase<THunkStorageNode>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
