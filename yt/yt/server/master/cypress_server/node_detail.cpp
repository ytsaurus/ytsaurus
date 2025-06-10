#include "node_detail.h"

#include "cypress_manager.h"
#include "helpers.h"
#include "node_proxy_detail.h"
#include "portal_exit_node.h"
#include "scion_node.h"
#include "resolve_cache.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chaos_server/chaos_manager.h>

#include <yt/yt/server/master/maintenance_tracker_server/cluster_proxy_node.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NChaosServer;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeTypeHandlerBase::TNontemplateCypressNodeTypeHandlerBase(
    NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

ETypeFlags TNontemplateCypressNodeTypeHandlerBase::GetFlags() const
{
    return
        ETypeFlags::ReplicateAttributes |
        ETypeFlags::ReplicateDestroy |
        ETypeFlags::Creatable;
}

void TNontemplateCypressNodeTypeHandlerBase::FillAttributes(
    TCypressNode* trunkNode,
    IAttributeDictionary* inheritedAttributes,
    IAttributeDictionary* explicitAttributes)
{
    for (const auto& key : inheritedAttributes->ListKeys()) {
        if (!IsSupportedInheritableAttribute(key)) {
            inheritedAttributes->Remove(key);
        }
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto combinedAttributes = NYTree::OverlayAttributeDictionaries(explicitAttributes, inheritedAttributes);
    objectManager->FillAttributes(trunkNode, *combinedAttributes);
}

bool TNontemplateCypressNodeTypeHandlerBase::IsSupportedInheritableAttribute(const std::string& /*key*/) const
{
    // NB: most node types don't inherit attributes. That would lead to
    // a lot of pseudo-user attributes.
    return false;
}

TAcdList TNontemplateCypressNodeTypeHandlerBase::ListAcds(TCypressNode* trunkNode) const
{
    return {&trunkNode->Acd()};
}

void TNontemplateCypressNodeTypeHandlerBase::SetBootstrap(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);
    Bootstrap_ = bootstrap;
}

TBootstrap* TNontemplateCypressNodeTypeHandlerBase::GetBootstrap() const
{
    YT_VERIFY(Bootstrap_);
    return Bootstrap_;
}

bool TNontemplateCypressNodeTypeHandlerBase::IsLeader() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsLeader();
}

bool TNontemplateCypressNodeTypeHandlerBase::IsRecovery() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

const TDynamicCypressManagerConfigPtr& TNontemplateCypressNodeTypeHandlerBase::GetDynamicCypressManagerConfig() const
{
    return Bootstrap_->GetConfigManager()->GetConfig()->CypressManager;
}

void TNontemplateCypressNodeTypeHandlerBase::ZombifyCorePrologue(TCypressNode* node)
{
    if (node->IsTrunk()) {
        // Invalidate resolve cache.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& resolveCache = cypressManager->GetResolveCache();
        resolveCache->InvalidateNode(node);
    }
}

void TNontemplateCypressNodeTypeHandlerBase::DestroyCorePrologue(TCypressNode* node)
{
    node->SetParent(nullptr);

    if (node->IsTrunk()) {
        // Reset reference to shard.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->ResetShard(node);
    }

    // Clear ACDs to unregister the node from linked objects.
    for (auto& acd : ListAcds(node)) {
        acd->Clear();
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->ResetAccount(node);
}

void TNontemplateCypressNodeTypeHandlerBase::SerializeNodeCore(
    TCypressNode* node,
    TSerializeNodeContext* context)
{
    using NYT::Save;

    auto erasedType = node->GetType();
    if (erasedType == EObjectType::PortalExit) {
        erasedType = EObjectType::MapNode;
    } else if (erasedType == EObjectType::Scion) {
        erasedType = EObjectType::SequoiaMapNode;
    }

    // These are loaded in TCypressManager::TNodeFactory::MaterializeNode.
    Save(*context, node->GetId());

    // These are loaded in TCypressManager::MaterializeNode.
    Save(*context, erasedType);
    Save(*context, node->GetExternalCellTag());

    // These are loaded in type handler.
    Save(*context, node->Account());
    Save(*context, node->GetTotalResourceUsage());
    // NB: ACDs can not be set under transaction.
    Save(*context, node->GetTrunkNode()->Acd());
    Save(*context, node->GetOpaque());
    Save(*context, node->TryGetAnnotation());
    Save(*context, node->GetCreationTime());
    Save(*context, node->GetModificationTime());
    Save(*context, node->TryGetExpirationTime());
    Save(*context, node->TryGetExpirationTimeout());

    // User attributes.
    auto keyToAttribute = GetNodeAttributes(
        Bootstrap_->GetCypressManager(),
        node->GetTrunkNode(),
        node->GetTransaction());
    Save(*context, SortHashMapByKeys(keyToAttribute));
}

TCypressNode* TNontemplateCypressNodeTypeHandlerBase::MaterializeNodeCore(
    TMaterializeNodeContext* context,
    ICypressNodeFactory* factory)
{
    auto sourceAccount = Load<TAccountRawPtr>(*context);
    auto sourceResourceUsage = Load<TClusterResources>(*context);
    auto sourceAcd = Load<TAccessControlDescriptor>(*context);
    auto opaque = Load<bool>(*context);
    auto optionalAnnotation = Load<std::optional<std::string>>(*context);
    auto creationTime = Load<TInstant>(*context);
    auto modificationTime = Load<TInstant>(*context);
    auto expirationTime = Load<std::optional<TInstant>>(*context);
    auto expirationTimeout = Load<std::optional<TDuration>>(*context);
    auto keyToAttribute = Load<std::vector<std::pair<std::string, TYsonString>>>(*context);

    auto* clonedAccount = factory->GetClonedNodeAccount(sourceAccount);
    factory->ValidateClonedAccount(
        context->GetMode(),
        sourceAccount,
        sourceResourceUsage,
        clonedAccount);

    // See SerializeNodeCore.
   TCypressNode* trunkNode = nullptr;
    if (auto targetNodeId = context->GetInplaceLoadTargetNodeId()) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        // You might wonder if trying to load data into an existing trunk node is a bad idea. Especially, since in the case when
        // externalization fails after the data is partially loaded, the node can end up in a weird state
        // with some changes rolled back and some not. You would be correct. The only saving grace is that
        // this code should only be called for node externalization, which creates the node in question inside of a transaction.
        // Thus, when transaction is aborted we also should destroy the node and actually roll all of the changes back.
        trunkNode = cypressManager->GetNodeOrThrow({targetNodeId, NullTransactionId});
    } else {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto clonedId = context->GetHintId()
            ? context->GetHintId()
            : objectManager->GenerateId(GetObjectType());
        trunkNode = factory->InstantiateNode(clonedId, context->GetExternalCellTag());
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->SetAccount(
        trunkNode,
        clonedAccount,
        /*transaction*/ nullptr);

    if (factory->ShouldPreserveAcl(context->GetMode()) && trunkNode->GetType() != EObjectType::PortalExit) {
        trunkNode->Acd().SetInherit(sourceAcd.Inherit());
        trunkNode->Acd().SetEntries(sourceAcd.Acl());
    }

    // Set owner.
    if (factory->ShouldPreserveOwner()) {
        trunkNode->Acd().SetOwner(sourceAcd.GetOwner());
    } else {
        auto* user = securityManager->GetAuthenticatedUser();
        trunkNode->Acd().SetOwner(user);
    }

    // Copy opaque.
    trunkNode->SetOpaque(opaque);

    // Copy annotation.
    if (optionalAnnotation) {
        trunkNode->SetAnnotation(std::move(*optionalAnnotation));
    } else {
        trunkNode->RemoveAnnotation();
    }

    // Copy creation time.
    if (factory->ShouldPreserveCreationTime()) {
        trunkNode->SetCreationTime(creationTime);
    }

    // Copy modification time.
    if (factory->ShouldPreserveModificationTime()) {
        trunkNode->SetModificationTime(modificationTime);
    }

    // Copy expiration time.
    if (factory->ShouldPreserveExpirationTime() && expirationTime) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetExpirationTime(trunkNode, expirationTime);
    }

    // Copy expiration timeout.
    if (factory->ShouldPreserveExpirationTimeout() && expirationTimeout) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetExpirationTimeout(trunkNode, expirationTimeout);
    }

    // Copy attributes directly to suppress validation.
    if (!keyToAttribute.empty()) {
        auto* clonedAttributes = trunkNode->GetMutableAttributes();
        const auto& ysonInternRegistry = Bootstrap_->GetYsonInternRegistry();
        for (const auto& [key, value] : keyToAttribute) {
            // NB: overwriting already existing attributes is essential in the inplace case.
            clonedAttributes->Set(key, ysonInternRegistry->Intern(value));
        }
    }

    return trunkNode;
}

void TNontemplateCypressNodeTypeHandlerBase::BranchCorePrologue(
    TCypressNode* originatingNode,
    TCypressNode* branchedNode,
    TTransaction* transaction,
    const TLockRequest& lockRequest)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();

    // Invalidate resolve cache.
    if (lockRequest.Mode == ELockMode::Exclusive) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& resolveCache = cypressManager->GetResolveCache();
        resolveCache->InvalidateNode(originatingNode);
    }

    // Copy sequoia properties.
    if (originatingNode->IsSequoia() && originatingNode->IsNative() && originatingNode->MutableSequoiaProperties()) {
        YT_VERIFY(!originatingNode->MutableSequoiaProperties()->Tombstone);
        YT_VERIFY(!originatingNode->MutableSequoiaProperties()->BeingCreated);

        branchedNode->ImmutableSequoiaProperties() =
            std::make_unique<TCypressNode::TImmutableSequoiaProperties>(*originatingNode->ImmutableSequoiaProperties());
        branchedNode->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>();
    }

    // Copy basic properties.
    branchedNode->SetParent(originatingNode->GetParent());
    branchedNode->SetCreationTime(originatingNode->GetCreationTime());
    branchedNode->SetModificationTime(originatingNode->GetModificationTime());
    branchedNode->SetAttributeRevision(originatingNode->GetAttributeRevision());
    branchedNode->SetContentRevision(originatingNode->GetContentRevision());
    branchedNode->SetLockMode(lockRequest.Mode);
    branchedNode->SetTrunkNode(originatingNode->GetTrunkNode());
    branchedNode->SetTransaction(transaction);
    branchedNode->SetOriginator(originatingNode);
    branchedNode->SetExternalCellTag(originatingNode->GetExternalCellTag());
    if (originatingNode->IsForeign()) {
        branchedNode->SetForeign();
        branchedNode->SetNativeContentRevision(originatingNode->GetNativeContentRevision());
    }
    branchedNode->SetOpaque(originatingNode->GetOpaque());
    branchedNode->SetReachable(originatingNode->GetReachable());

    // Copying node's account requires special handling.
    YT_VERIFY(!branchedNode->Account());
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* account = originatingNode->Account().Get();
    securityManager->SetAccount(branchedNode, account, transaction);

    // Branch user attributes.
    objectManager->BranchAttributes(originatingNode, branchedNode);
}

void TNontemplateCypressNodeTypeHandlerBase::BranchCoreEpilogue(
    TCypressNode* branchedNode)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(branchedNode);
}

void TNontemplateCypressNodeTypeHandlerBase::MergeCorePrologue(
    TCypressNode* originatingNode,
    TCypressNode* branchedNode)
{
    YT_VERIFY(static_cast<bool>(originatingNode->MutableSequoiaProperties()) == static_cast<bool>(branchedNode->MutableSequoiaProperties()));

    // Copy Sequoia properties.
    if (originatingNode->IsSequoia() && originatingNode->IsNative() && originatingNode->MutableSequoiaProperties()) {
        // Just a sanity check that properties did not change.
        YT_VERIFY(*originatingNode->ImmutableSequoiaProperties() == *branchedNode->ImmutableSequoiaProperties());
        YT_VERIFY(!originatingNode->MutableSequoiaProperties()->Tombstone);
    }

    // Perform cleanup by resetting the parent link of the branched node.
    branchedNode->SetParent(nullptr);

    // Perform cleanup by resetting the account of the branched node.
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->ResetAccount(branchedNode);

    // Merge user attributes.
    const auto& objectManager = Bootstrap_->GetObjectManager();
    objectManager->MergeAttributes(originatingNode, branchedNode);
    originatingNode->MergeAnnotation(branchedNode);

    // Merge modification time.
    const auto* mutationContext = NHydra::GetCurrentMutationContext();
    originatingNode->SetModificationTime(std::max(originatingNode->GetModificationTime(), branchedNode->GetModificationTime()));
    originatingNode->SetAttributeRevision(mutationContext->GetVersion().ToRevision());
    originatingNode->SetContentRevision(mutationContext->GetVersion().ToRevision());
    if (originatingNode->IsForeign()) {
        auto* transaction = branchedNode->GetTransaction();
        auto nativeContentRevision = transaction->GetNativeCommitMutationRevision();
        if (branchedNode->GetNativeContentRevision() <= nativeContentRevision) {
            originatingNode->SetNativeContentRevision(nativeContentRevision);
        } else {
            YT_LOG_ALERT("Received non-monotonic native content revision update; ignored (NodeId: %v, OldNativeContentRevision: %x, NewNativeContentRevision: %x)",
                branchedNode->GetVersionedId(),
                branchedNode->GetNativeContentRevision(),
                nativeContentRevision);
        }
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    cypressManager->MergeExpirationTime(originatingNode, branchedNode);
    cypressManager->MergeExpirationTimeout(originatingNode, branchedNode);
}

void TNontemplateCypressNodeTypeHandlerBase::MergeCoreEpilogue(
    TCypressNode* originatingNode,
    TCypressNode* branchedNode)
{
    // Update only originating node since ResetAccount was called for branched node.
    const auto& securityManager = Bootstrap_->GetSecurityManager();

    securityManager->ResetAccount(branchedNode);

    securityManager->UpdateMasterMemoryUsage(originatingNode);
}

TCypressNode* TNontemplateCypressNodeTypeHandlerBase::CloneCorePrologue(
    ICypressNodeFactory* factory,
    TNodeId hintId,
    TCypressNode* sourceNode,
    TAccount* account)
{
    auto type = GetObjectType();
    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto clonedId = hintId
        ? hintId
        : objectManager->GenerateId(type);

    auto* clonedTrunkNode = factory->InstantiateNode(clonedId, sourceNode->GetExternalCellTag());

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->SetAccount(
        clonedTrunkNode,
        account,
        /*transaction*/ nullptr);

    return clonedTrunkNode;
}

void TNontemplateCypressNodeTypeHandlerBase::CloneCoreEpilogue(
    TCypressNode* sourceNode,
    TCypressNode* clonedTrunkNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode)
{
    // Copy attributes directly to suppress validation.
    auto keyToAttribute = GetNodeAttributes(
        Bootstrap_->GetCypressManager(),
        sourceNode->GetTrunkNode(),
        factory->GetTransaction());
    if (!keyToAttribute.empty()) {
        auto* clonedAttributes = clonedTrunkNode->GetMutableAttributes();
        for (const auto& [key, value] : keyToAttribute) {
            YT_VERIFY(clonedAttributes->TryInsert(key, value));
        }
    }

    // Copy ACD.
    if (factory->ShouldPreserveAcl(mode)) {
        clonedTrunkNode->Acd().SetInherit(sourceNode->Acd().Inherit());
        clonedTrunkNode->Acd().SetEntries(sourceNode->Acd().Acl());
    }

    // Copy builtin attributes.
    clonedTrunkNode->SetOpaque(sourceNode->GetOpaque());
    if (auto optionalAnnotation = sourceNode->TryGetAnnotation()) {
        clonedTrunkNode->SetAnnotation(*optionalAnnotation);
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(clonedTrunkNode);
}

void TNontemplateCypressNodeTypeHandlerBase::RefObject(TCypressNode* node)
{
    Bootstrap_->GetObjectManager()->RefObject(node);
}

void TNontemplateCypressNodeTypeHandlerBase::UnrefObject(TCypressNode* node)
{
    Bootstrap_->GetObjectManager()->UnrefObject(node);
}

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
void TCompositeCypressNodeTypeHandlerBase<TImpl>::DoDestroy(TImpl* node)
{
    TBase::DoDestroy(node);

    // Drop parent links from immediate descendants.
    for (auto* descendant : node->ImmediateDescendants()) {
        descendant->DropParent();
    }
    node->ImmediateDescendants().clear();
}

template <class TImpl>
void TCompositeCypressNodeTypeHandlerBase<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    IAttributeDictionary* inheritedAttributes,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    clonedTrunkNode->CloneAttributesFrom(sourceNode);
}

template <class TImpl>
void TCompositeCypressNodeTypeHandlerBase<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    // NB: leaving branch's Attributes_ null here.
}

template <class TImpl>
void TCompositeCypressNodeTypeHandlerBase<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    originatingNode->MergeAttributesFrom(branchedNode);
}

template <class TImpl>
bool TCompositeCypressNodeTypeHandlerBase<TImpl>::HasBranchedChangesImpl(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    auto* branchedAttributes = originatingNode->FindAttributes();
    return branchedAttributes && !branchedAttributes->AreEmpty();
}

template <class TImpl>
void TCompositeCypressNodeTypeHandlerBase<TImpl>::DoSerializeNode(
    TImpl* node,
    TSerializeNodeContext* context)
{
    TBase::DoSerializeNode(node, context);

    using NYT::Save;
    TCompositeCypressNode::TTransientAttributes attributes;
    // NB: Using ENodeMaterializationReason::Create here, since we need a full list of attributes for each node.
    node->FillInheritableAttributes(&attributes, ENodeMaterializationReason::Create);
    Save(*context, !attributes.AreEmpty());
    if (!attributes.AreEmpty()) {
        Save(*context, attributes);
    }
}

template <class TImpl>
void TCompositeCypressNodeTypeHandlerBase<TImpl>::DoMaterializeNode(
    TImpl* trunkNode,
    TMaterializeNodeContext* context)
{
    TBase::DoMaterializeNode(trunkNode, context);

    using NYT::Load;
    if (Load<bool>(*context)) {
        auto attributes = Load<TCompositeCypressNode::TTransientAttributes>(*context);
        auto persistentAttributes = attributes.ToPersistent();
        trunkNode->SetAttributes(&persistentAttributes);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TNonOwnedChild>
bool TMapNodeChildren<TNonOwnedChild>::IsNull(TNonOwnedChild child) noexcept
{
    if constexpr (ChildIsPointer) {
        return child == nullptr;
    } else {
        static_assert(std::is_same_v<TNonOwnedChild, TGuid>);
        return child == TGuid();
    }
}

template <class TNonOwnedChild>
typename TMapNodeChildren<TNonOwnedChild>::TMaybeOwnedChild
TMapNodeChildren<TNonOwnedChild>::ToOwnedOnLoad(TNonOwnedChild child)
{
    if constexpr (ChildIsPointer) {
        return TMaybeOwnedChild(child, TObjectPtrLoadTag());
    } else {
        return child;
    }
}

template <class TNonOwnedChild>
typename TMapNodeChildren<TNonOwnedChild>::TMaybeOwnedChild
TMapNodeChildren<TNonOwnedChild>::Clone(const TMaybeOwnedChild& child)
{
    if constexpr (ChildIsPointer) {
        return child.Clone();
    } else {
        return child;
    }
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::MaybeVerifyIsTrunk(TNonOwnedChild child)
{
    if constexpr (ChildIsPointer) {
        YT_VERIFY(!child || child->IsTrunk());
    }
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, KeyToChild_);
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, KeyToChild_);

    // Reconstruct ChildToKey map.
    for (const auto& [key, childNode] : KeyToChild_) {
        if (!IsNull(childNode)) {
            EmplaceOrCrash(ChildToKey_, ToOwnedOnLoad(childNode), key);
        }
    }

    RecomputeMasterMemoryUsage();
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::RecomputeMasterMemoryUsage()
{
    MasterMemoryUsage_ = 0;
    for (const auto& [key, childNode] : KeyToChild_) {
        MasterMemoryUsage_ += std::ssize(key);
    }
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Set(const std::string& key, TNonOwnedChild child)
{
    MaybeVerifyIsTrunk(child);

    auto it = KeyToChild_.find(key);
    if (it == KeyToChild_.end()) {
        MasterMemoryUsage_ += std::ssize(key);
    } else if (it->second) {
        if (it->second == child) {
            return;
        }
        EraseOrCrash(ChildToKey_, it->second);
    }

    KeyToChild_[key] = child;
    if (child) {
        EmplaceOrCrash(ChildToKey_, child, key);
    }
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Insert(const std::string& key, TNonOwnedChild child)
{
    MaybeVerifyIsTrunk(child);

    EmplaceOrCrash(KeyToChild_, key, child);
    MasterMemoryUsage_ += std::ssize(key);

    if (child) {
        EmplaceOrCrash(ChildToKey_, child, key);
    }
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Remove(const std::string& key, TNonOwnedChild child)
{
    MaybeVerifyIsTrunk(child);

    auto it = KeyToChild_.find(key);
    YT_VERIFY(it != KeyToChild_.end());
    YT_VERIFY(it->second == child);
    MasterMemoryUsage_ -= std::ssize(key);
    KeyToChild_.erase(it);
    if (child) {
        EraseOrCrash(ChildToKey_, child);
    }
}

template <class TNonOwnedChild>
bool TMapNodeChildren<TNonOwnedChild>::Contains(const std::string& key) const
{
    return KeyToChild_.find(key) != KeyToChild_.end();
}

template <class TNonOwnedChild>
const typename TMapNodeChildren<TNonOwnedChild>::TKeyToChild& TMapNodeChildren<TNonOwnedChild>::KeyToChild() const
{
    return KeyToChild_;
}

template <class TNonOwnedChild>
const typename TMapNodeChildren<TNonOwnedChild>::TChildToKey& TMapNodeChildren<TNonOwnedChild>::ChildToKey() const
{
    return ChildToKey_;
}

template <class TNonOwnedChild>
int TMapNodeChildren<TNonOwnedChild>::GetRefCount() const noexcept
{
    return RefCount_;
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Ref() noexcept
{
    ++RefCount_;
}

template <class TNonOwnedChild>
void TMapNodeChildren<TNonOwnedChild>::Unref() noexcept
{
    YT_VERIFY(--RefCount_ >= 0);
}

template <class TNonOwnedChild>
/*static*/ std::unique_ptr<TMapNodeChildren<TNonOwnedChild>> TMapNodeChildren<TNonOwnedChild>::Copy(
    TMapNodeChildren* srcChildren)
{
    YT_VERIFY(srcChildren->GetRefCount() != 0);

    auto dstChildren = std::make_unique<TMapNodeChildren>();

    dstChildren->KeyToChild_ = srcChildren->KeyToChild_;
    // NB: The order of refs here is non-deterministic but this should not be a problem.
    dstChildren->ChildToKey_.reserve(srcChildren->ChildToKey_.size());
    for (const auto& [child, key] : srcChildren->ChildToKey_) {
        dstChildren->ChildToKey_.emplace(Clone(child), key);
    }

    dstChildren->RecomputeMasterMemoryUsage();

    return dstChildren;
}

template class TMapNodeChildren<TCypressNodeRawPtr>;
template class TMapNodeChildren<TNodeId>;

////////////////////////////////////////////////////////////////////////////////

template <class TChild>
const typename TMapNodeImpl<TChild>::TKeyToChild& TMapNodeImpl<TChild>::KeyToChild() const
{
    return Children_.Get().KeyToChild();
}

template <class TChild>
const typename TMapNodeImpl<TChild>::TChildToKey& TMapNodeImpl<TChild>::ChildToKey() const
{
    return Children_.Get().ChildToKey();
}

template <class TChild>
typename TMapNodeImpl<TChild>::TChildren& TMapNodeImpl<TChild>::MutableChildren()
{
    return Children_.MutableGet();
}

template <class TChild>
ENodeType TMapNodeImpl<TChild>::GetNodeType() const
{
    if constexpr (std::is_same_v<TChild, TNodeId>) {
        return ENodeType::Entity;
    } else {
        return ENodeType::Map;
    }
}

template <class TChild>
void TMapNodeImpl<TChild>::Save(NCellMaster::TSaveContext& context) const
{
    TCompositeCypressNode::Save(context);

    using NYT::Save;
    Save(context, ChildCountDelta_);
    Save(context, Children_);
}

template <class TChild>
void TMapNodeImpl<TChild>::Load(NCellMaster::TLoadContext& context)
{
    TCompositeCypressNode::Load(context);

    using NYT::Load;

    Load(context, ChildCountDelta_);
    Load(context, Children_);
}

template <class TChild>
int TMapNodeImpl<TChild>::GetGCWeight() const
{
    if constexpr (TChildren::ChildIsPointer) {
        return TObject::GetGCWeight() + KeyToChild().size();
    } else {
        return TCompositeCypressNode::GetGCWeight();
    }
}

template <class TChild>
TDetailedMasterMemory TMapNodeImpl<TChild>::GetDetailedMasterMemoryUsage() const
{
    auto result = TCompositeCypressNode::GetDetailedMasterMemoryUsage();
    result[EMasterMemoryType::Nodes] += Children_.Get().GetMasterMemoryUsage();
    return result;
}

template <class TChild>
void TMapNodeImpl<TChild>::AssignChildren(const TObjectPartCoWPtr<TChildren>& children)
{
    Children_.Assign(children);
}

template <class TChild>
uintptr_t TMapNodeImpl<TChild>::GetMapNodeChildrenAddress() const
{
    return reinterpret_cast<uintptr_t>(&Children_.Get());
}

template <class TChild>
bool TMapNodeImpl<TChild>::IsNull(TChild child) noexcept
{
    return TChildren::IsNull(child);
}

template class TMapNodeImpl<TCypressNodeRawPtr>;
template class TMapNodeImpl<TNodeId>;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
EObjectType TCypressMapNodeTypeHandlerImpl<TImpl>::GetObjectType() const
{
    return EObjectType::MapNode;
}

template <class TImpl>
ENodeType TCypressMapNodeTypeHandlerImpl<TImpl>::GetNodeType() const
{
    if constexpr (std::is_same_v<TImpl, TCypressNode*>) {
        return ENodeType::Map;
    } else {
        return ENodeType::Entity;
    }
}

template <class TImpl>
ICypressNodeProxyPtr TCypressMapNodeTypeHandlerImpl<TImpl>::DoGetProxy(
    TImpl* trunkNode,
    TTransaction* transaction)
{
    return New<TCypressMapNodeProxy>(
        this->GetBootstrap(),
        &this->Metadata_,
        transaction,
        trunkNode);
}

template <class TImpl>
void TCypressMapNodeTypeHandlerImpl<TImpl>::DoDestroy(TImpl* node)
{
    node->ChildCountDelta_ = 0;
    node->Children_.Reset();

    TBase::DoDestroy(node);
}

template <class TImpl>
void TCypressMapNodeTypeHandlerImpl<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    YT_VERIFY(!branchedNode->Children_);

    if (lockRequest.Mode == ELockMode::Snapshot) {
        if (originatingNode->IsTrunk()) {
            branchedNode->ChildCountDelta() = originatingNode->ChildCountDelta();
            branchedNode->AssignChildren(originatingNode->Children_);
        } else {
            const auto& cypressManager = this->GetBootstrap()->GetCypressManager();

            TKeyToCypressNode keyToChildStorage;
            const auto& originatingNodeChildren = GetMapNodeChildMap(
                cypressManager,
                originatingNode->GetTrunkNode()->template As<TImpl>(),
                originatingNode->GetTransaction(),
                &keyToChildStorage);

            branchedNode->ChildCountDelta() = originatingNodeChildren.size();
            auto& children = branchedNode->MutableChildren();
            for (const auto& [key, childNode] : SortHashMapByKeys(originatingNodeChildren)) {
                children.Insert(key, childNode);
            }
        }
    }

    // Non-snapshot branches only hold changes, i.e. deltas. Which are empty at first.
}

template <class TImpl>
void TCypressMapNodeTypeHandlerImpl<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    bool isOriginatingNodeBranched = originatingNode->GetTransaction() != nullptr;

    auto& children = originatingNode->MutableChildren();
    const auto& keyToChild = originatingNode->KeyToChild();

    const auto& cypressManager = this->GetBootstrap()->GetCypressManager();
    auto parentIsReachable = originatingNode->GetReachable();

    auto maybeSetUnrechableOverwrittenOriginatingNodeChild = [&] (TCypressNode* child) {
        if (parentIsReachable) {
            cypressManager->SetUnreachableSubtreeNodes(child, originatingNode->GetTransaction());
        }
    };
    auto maybeSetRechableOverwritingBranchedNodeChild = [&] (TCypressNode* child) {
        if (parentIsReachable) {
            cypressManager->SetReachableSubtreeNodes(child, originatingNode->GetTransaction());
        }
    };

    for (const auto& [key, trunkChildNode] : SortHashMapByKeys(branchedNode->KeyToChild())) {
        auto it = keyToChild.find(key);
        auto childModified = it == keyToChild.end() || it->second != trunkChildNode;
        if (trunkChildNode && childModified) {
            if (it != keyToChild.end() && it->second) {
                // Originating node's child by this key is removed and becomes unreachable.
                maybeSetUnrechableOverwrittenOriginatingNodeChild(it->second);
            }

            // The following action invalidates the iterator.
            children.Set(key, trunkChildNode);

            // Branched subtree becomes reachable under the originating node's transaction.
            maybeSetRechableOverwritingBranchedNodeChild(trunkChildNode);
        } else if (!trunkChildNode) {
            // Branched: tombstone
            if (it == keyToChild.end()) {
                // Originating: missing
                if (isOriginatingNodeBranched) {
                    children.Insert(key, nullptr);
                }
            } else if (it->second) {
                // Originating: present
                maybeSetUnrechableOverwrittenOriginatingNodeChild(it->second);

                if (isOriginatingNodeBranched) {
                    children.Set(key, nullptr);
                } else {
                    children.Remove(key, it->second);
                }
            } else {
                // Originating: tombstone
            }
        }
    }

    originatingNode->ChildCountDelta() += branchedNode->ChildCountDelta();

    branchedNode->Children_.Reset();
}

template <class TImpl>
void TCypressMapNodeTypeHandlerImpl<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    IAttributeDictionary* inheritedAttributes,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    auto* transaction = factory->GetTransaction();

    const auto& cypressManager = this->GetBootstrap()->GetCypressManager();

    TKeyToCypressNode keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        sourceNode->GetTrunkNode()->template As<TCypressMapNode>(),
        transaction,
        &keyToChildMapStorage);
    auto keyToChildList = SortHashMapByKeys(keyToChildMap);

    auto& clonedChildren = clonedTrunkNode->MutableChildren();

    for (const auto& [key, trunkChildNode] : keyToChildList) {
        auto* childNode = cypressManager->GetVersionedNode(trunkChildNode, transaction);

        auto* clonedChildNode = factory->CloneNode(childNode, mode, inheritedAttributes);
        auto* clonedTrunkChildNode = clonedChildNode->GetTrunkNode();

        clonedChildren.Insert(key, clonedTrunkChildNode);

        AttachChildToNode(clonedTrunkNode, clonedChildNode);

        ++clonedTrunkNode->ChildCountDelta();
    }
}

template <class TImpl>
bool TCypressMapNodeTypeHandlerImpl<TImpl>::HasBranchedChangesImpl(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    if (branchedNode->GetLockMode() == ELockMode::Snapshot) {
        return false;
    }

    return !branchedNode->KeyToChild().empty();
}

template <class TImpl>
void TCypressMapNodeTypeHandlerImpl<TImpl>::DoSerializeNode(
    TImpl* node,
    TSerializeNodeContext* context)
{
    TBase::DoSerializeNode(node, context);

    using NYT::Save;

    const auto& cypressManager = this->GetBootstrap()->GetCypressManager();

    TKeyToCypressNode keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        node->GetTrunkNode()->template As<TImpl>(),
        node->GetTransaction(),
        &keyToChildMapStorage);

    TSizeSerializer::Save(*context, keyToChildMap.size());
    for (const auto& [key, child] : SortHashMapByKeys(keyToChildMap)) {
        Save(*context, key);
        Save(*context, child->GetId());
    }
}

template <class TImpl>
void TCypressMapNodeTypeHandlerImpl<TImpl>::DoMaterializeNode(
    TImpl* trunkNode,
    TMaterializeNodeContext* context)
{
    TBase::DoMaterializeNode(trunkNode, context);

    using NYT::Load;

    size_t size = TSizeSerializer::Load(*context);
    for (size_t index = 0; index < size; ++index) {
        auto key = Load<TKeyToCypressNodeId::key_type>(*context);
        auto childId = Load<TKeyToCypressNodeId::mapped_type>(*context);
        context->RegisterChild(std::move(key), childId);
    }
}

// Explicit instantiations.
template class TCypressMapNodeTypeHandlerImpl<TCypressMapNode>;
template class TCypressMapNodeTypeHandlerImpl<TPortalExitNode>;
template class TCypressMapNodeTypeHandlerImpl<NMaintenanceTrackerServer::TClusterProxyNode>;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
EObjectType TSequoiaMapNodeTypeHandlerImpl<TImpl>::GetObjectType() const
{
    return EObjectType::SequoiaMapNode;
}

template <class TImpl>
ENodeType TSequoiaMapNodeTypeHandlerImpl<TImpl>::GetNodeType() const
{
    return ENodeType::Entity;
}

template <class TImpl>
ICypressNodeProxyPtr TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoGetProxy(
    TImpl* trunkNode,
    TTransaction* transaction)
{
    return New<TSequoiaMapNodeProxy>(
        this->GetBootstrap(),
        &this->Metadata_,
        transaction,
        trunkNode);
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoDestroy(TImpl* trunkNode)
{
    trunkNode->ChildCountDelta_ = 0;
    trunkNode->Children_.Reset();

    TBase::DoDestroy(trunkNode);
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    YT_VERIFY(!branchedNode->Children_);

    if (lockRequest.Mode == ELockMode::Snapshot) {
        // TODO(kvk1920): don't copy children since accessing node via its
        // parent snapshot is forbidden in Sequoia.
        if (originatingNode->IsTrunk()) {
            branchedNode->ChildCountDelta() = originatingNode->ChildCountDelta();
            branchedNode->AssignChildren(originatingNode->Children_);
        } else {
            const auto& cypressManager = this->GetBootstrap()->GetCypressManager();

            TKeyToCypressNodeId keyToChildStorage;
            const auto& originatingNodeChildren = GetMapNodeChildMap(
                cypressManager,
                originatingNode->GetTrunkNode()->template As<TImpl>(),
                originatingNode->GetTransaction(),
                &keyToChildStorage);

            branchedNode->ChildCountDelta() = originatingNodeChildren.size();
            auto& children = branchedNode->MutableChildren();
            for (const auto& [key, childNode] : SortHashMapByKeys(originatingNodeChildren)) {
                children.Insert(key, childNode);
            }
        }
    }
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    bool isOriginatingNodeBranched = originatingNode->GetTransaction() != nullptr;

    auto& children = originatingNode->MutableChildren();
    const auto& keyToChild = originatingNode->KeyToChild();

    for (const auto& [key, trunkChildNode] : SortHashMapByKeys(branchedNode->KeyToChild())) {
        auto it = keyToChild.find(key);
        auto childModified = it == keyToChild.end() || it->second != trunkChildNode;
        if (trunkChildNode && childModified) {
            // The following action invalidates the iterator.
            children.Set(key, trunkChildNode);
        } else if (!trunkChildNode) {
            // Branched: tombstone
            if (it == keyToChild.end()) {
                // Originating: missing
                if (isOriginatingNodeBranched) {
                    children.Insert(key, {});
                }
            } else if (it->second) {
                // Originating: present
                if (isOriginatingNodeBranched) {
                    children.Set(key, {});
                } else {
                    children.Remove(key, it->second);
                }
            } else {
                // Originating: tombstone
            }
        }
    }

    originatingNode->ChildCountDelta() += branchedNode->ChildCountDelta();

    branchedNode->Children_.Reset();
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    IAttributeDictionary* inheritedAttributes,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    auto* transaction = factory->GetTransaction();

    const auto& cypressManager = this->GetBootstrap()->GetCypressManager();

    TKeyToCypressNodeId keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        sourceNode->GetTrunkNode()->template As<TSequoiaMapNode>(),
        transaction,
        &keyToChildMapStorage);
    auto keyToChildList = SortHashMapByKeys(keyToChildMap);

    auto& clonedChildren = clonedTrunkNode->MutableChildren();

    for (const auto& [key, trunkChildNode] : keyToChildList) {
        clonedChildren.Insert(key, trunkChildNode);
        ++clonedTrunkNode->ChildCountDelta();
    }
}

template <class TImpl>
bool TSequoiaMapNodeTypeHandlerImpl<TImpl>::HasBranchedChangesImpl(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    if (branchedNode->GetLockMode() == ELockMode::Snapshot) {
        return false;
    }

    return !branchedNode->KeyToChild().empty();
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoSerializeNode(
    TImpl* node,
    TSerializeNodeContext* context)
{
    TBase::DoSerializeNode(node, context);

    using NYT::Save;

    const auto& cypressManager = this->GetBootstrap()->GetCypressManager();
    TKeyToCypressNodeId keyToChildMapStorage;
    const auto& keyToChildIdMap = GetMapNodeChildMap(
        cypressManager,
        node->GetTrunkNode()->template As<TImpl>(),
        node->GetTransaction(),
        &keyToChildMapStorage);

    TSizeSerializer::Save(*context, keyToChildIdMap.size());
    for (const auto& [key, childId] : SortHashMapByKeys(keyToChildIdMap)) {
        Save(*context, key);
        Save(*context, childId);
    }
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoMaterializeNode(
    TImpl* trunkNode,
    TMaterializeNodeContext* context)
{
    TBase::DoMaterializeNode(trunkNode, context);

    using NYT::Load;

    size_t size = TSizeSerializer::Load(*context);
    for (size_t index = 0; index < size; ++index) {
        auto key = Load<TKeyToCypressNodeId::key_type>(*context);
        auto childId = Load<TKeyToCypressNodeId::mapped_type>(*context);
        context->RegisterChild(std::move(key), childId);
    }
}

// Explicit instantiations.
template class TSequoiaMapNodeTypeHandlerImpl<TSequoiaMapNode>;
template class TSequoiaMapNodeTypeHandlerImpl<TScionNode>;

////////////////////////////////////////////////////////////////////////////////

ENodeType TListNode::GetNodeType() const
{
    return ENodeType::List;
}

void TListNode::Save(NCellMaster::TSaveContext& context) const
{
    TCompositeCypressNode::Save(context);

    SaveWith<TVectorSerializer<TRawNonversionedObjectPtrSerializer>>(context, IndexToChild_);
}

void TListNode::Load(NCellMaster::TLoadContext& context)
{
    TCompositeCypressNode::Load(context);

    LoadWith<TVectorSerializer<TRawNonversionedObjectPtrSerializer>>(context, IndexToChild_);

    // Reconstruct ChildToIndex_.
    for (int index = 0; index < std::ssize(IndexToChild_); ++index) {
        YT_VERIFY(ChildToIndex_.emplace(IndexToChild_[index], index).second);
    }
}

int TListNode::GetGCWeight() const
{
    return TObject::GetGCWeight() + IndexToChild_.size();
}

////////////////////////////////////////////////////////////////////////////////

EObjectType TListNodeTypeHandler::GetObjectType() const
{
    return EObjectType::ListNode;
}

ENodeType TListNodeTypeHandler::GetNodeType() const
{
    return ENodeType::List;
}

std::unique_ptr<TCypressNode> TListNodeTypeHandler::Create(
    TNodeId hintId,
    const TCreateNodeContext& context)
{
    THROW_ERROR_EXCEPTION_IF(
        GetDynamicCypressManagerConfig()->ForbidListNodeCreation,
        "List nodes are deprecated and will be removed in the near future");

    return TBase::Create(hintId, context);
}

ICypressNodeProxyPtr TListNodeTypeHandler::DoGetProxy(
    TListNode* trunkNode,
    TTransaction* transaction)
{
    return New<TListNodeProxy>(
        GetBootstrap(),
        &Metadata_,
        transaction,
        trunkNode);
}

void TListNodeTypeHandler::DoDestroy(TListNode* node)
{
    // Drop references to the children.
    const auto& objectManager = GetBootstrap()->GetObjectManager();
    for (auto child : node->IndexToChild()) {
        objectManager->UnrefObject(child);
    }

    TBase::DoDestroy(node);
}

void TListNodeTypeHandler::DoBranch(
    const TListNode* originatingNode,
    TListNode* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    branchedNode->IndexToChild() = originatingNode->IndexToChild();
    branchedNode->ChildToIndex() = originatingNode->ChildToIndex();

    // Reference all children.
    const auto& objectManager = GetBootstrap()->GetObjectManager();
    for (auto child : originatingNode->IndexToChild()) {
        objectManager->RefObject(child);
    }
}

void TListNodeTypeHandler::DoMerge(
    TListNode* originatingNode,
    TListNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    // Drop all references held by the originator.
    const auto& objectManager = GetBootstrap()->GetObjectManager();
    for (auto child : originatingNode->IndexToChild()) {
        objectManager->UnrefObject(child);
    }

    const auto& cypressManager = this->GetBootstrap()->GetCypressManager();

    auto parentIsReachable = originatingNode->GetReachable();
    if (parentIsReachable) {
        // We do not care about performance here as list nodes are deprecated.
        cypressManager->SetUnreachableSubtreeNodes(originatingNode->GetTrunkNode(), originatingNode->GetTransaction());
    }

    // Replace the child list with the branched copy.
    originatingNode->IndexToChild().swap(branchedNode->IndexToChild());
    originatingNode->ChildToIndex().swap(branchedNode->ChildToIndex());

    if (parentIsReachable) {
        cypressManager->SetReachableSubtreeNodes(originatingNode->GetTrunkNode(), originatingNode->GetTransaction());
    }
}

void TListNodeTypeHandler::DoClone(
    TListNode* sourceNode,
    TListNode* clonedTrunkNode,
    IAttributeDictionary* inheritedAttributes,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, inheritedAttributes, factory, mode, account);

    const auto& objectManager = GetBootstrap()->GetObjectManager();
    const auto& indexToChild = sourceNode->IndexToChild();

    for (int index = 0; index < std::ssize(indexToChild); ++index) {
        auto childNode = indexToChild[index];
        auto* clonedChildNode = factory->CloneNode(childNode, mode, inheritedAttributes);
        auto* clonedChildTrunkNode = clonedChildNode->GetTrunkNode();

        clonedTrunkNode->IndexToChild().push_back(clonedChildTrunkNode);
        YT_VERIFY(clonedTrunkNode->ChildToIndex().emplace(clonedChildTrunkNode, index).second);

        AttachChildToNode(clonedTrunkNode, clonedChildNode);
        objectManager->RefObject(clonedChildNode->GetTrunkNode());
    }
}

bool TListNodeTypeHandler::HasBranchedChangesImpl(TListNode* originatingNode, TListNode* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    return branchedNode->IndexToChild() != originatingNode->IndexToChild();
}

void TListNodeTypeHandler::DoSerializeNode(
    TListNode* /*node*/,
    TSerializeNodeContext* /*context*/)
{
    THROW_ERROR_EXCEPTION("List nodes are deprecated and do not support cross-cell copying");
}

void TListNodeTypeHandler::DoMaterializeNode(
    TListNode* trunkNode,
    TMaterializeNodeContext* /*context*/)
{
    YT_LOG_ALERT("Received EndCopy command for list node, despite BeginCopy being disabled for this type (ListNodeId: %v)",
        trunkNode->GetId());
    THROW_ERROR_EXCEPTION("List nodes are deprecated and do not support cross-cell copying");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
