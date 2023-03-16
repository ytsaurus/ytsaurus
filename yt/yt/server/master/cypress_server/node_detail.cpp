#include "node_detail.h"

#include "helpers.h"
#include "node_proxy_detail.h"
#include "portal_exit_node.h"
#include "scion_node.h"
#include "shard.h"
#include "resolve_cache.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChaosServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

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

bool TNontemplateCypressNodeTypeHandlerBase::IsSupportedInheritableAttribute(const TString&) const
{
    // NB: most node types don't inherit attributes. That would lead to
    // a lot of pseudo-user attributes.
    return false;
}

TAcdList TNontemplateCypressNodeTypeHandlerBase::ListAcds(TCypressNode* trunkNode) const
{
    return {&trunkNode->Acd()};
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

void TNontemplateCypressNodeTypeHandlerBase::DestroyCorePrologue(TCypressNode* node)
{
    // Reset parent links from immediate descendants.
    for (auto* descendant : node->ImmediateDescendants()) {
        descendant->ResetParent();
    }
    node->ImmediateDescendants().clear();
    node->SetParent(nullptr);

    if (node->IsTrunk()) {
        // Reset reference to shard.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->ResetShard(node);

        // Invalidate resolve cache.
        const auto& resolveCache = cypressManager->GetResolveCache();
        resolveCache->InvalidateNode(node);
    }

    // Clear ACDs to unregister the node from linked objects.
    for (auto& acd : ListAcds(node)) {
        acd->Clear();
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->ResetAccount(node);
}

bool TNontemplateCypressNodeTypeHandlerBase::BeginCopyCore(
    TCypressNode* node,
    TBeginCopyContext* context)
{
    using NYT::Save;

    auto erasedType = node->GetType();
    if (erasedType == EObjectType::PortalExit) {
        erasedType = EObjectType::MapNode;
    }

    // These are loaded in TCypressManager::TNodeFactory::EndCopyNode.
    Save(*context, node->GetId());

    // These are loaded in TCypressManager::EndCopyNode.
    Save(*context, erasedType);

    // These are loaded in EndCopyCore.
    Save(*context, node->GetExternalCellTag());

    // These are loaded in type handler.
    Save(*context, node->Account().Get());
    Save(*context, node->GetTotalResourceUsage());
    Save(*context, node->Acd());
    Save(*context, node->GetOpaque());
    Save(*context, node->TryGetAnnotation());
    Save(*context, node->GetCreationTime());
    Save(*context, node->GetModificationTime());
    Save(*context, node->TryGetExpirationTime());
    Save(*context, node->TryGetExpirationTimeout());

    // User attributes
    auto keyToAttribute = GetNodeAttributes(
        Bootstrap_->GetCypressManager(),
        node->GetTrunkNode(),
        node->GetTransaction());
    Save(*context, SortHashMapByKeys(keyToAttribute));

    // For externalizable nodes, lock the source to ensure it survives until EndCopy.
    if (node->GetExternalCellTag() != NotReplicatedCellTagSentinel) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->LockNode(
            node->GetTrunkNode(),
            context->GetTransaction(),
            context->GetMode() == ENodeCloneMode::Copy ? ELockMode::Snapshot : ELockMode::Exclusive);
    }

    bool opaqueChild = false;
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    if (node->GetOpaque() && node != context->GetRootNode()) {
        context->RegisterOpaqueChildPath(cypressManager->GetNodePath(node, context->GetTransaction()));
        opaqueChild = true;
    }
    Save(*context, opaqueChild);

    return !opaqueChild;
}

TCypressNode* TNontemplateCypressNodeTypeHandlerBase::EndCopyCore(
    TEndCopyContext* context,
    ICypressNodeFactory* factory,
    TNodeId sourceNodeId,
    bool* needCustomEndCopy)
{
    // See BeginCopyCore.
    auto externalCellTag = Load<TCellTag>(*context);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (externalCellTag == multicellManager->GetCellTag()) {
        THROW_ERROR_EXCEPTION("Cannot copy node %v to cell %v since the latter is its external cell",
            sourceNodeId,
            externalCellTag);
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto clonedId = objectManager->GenerateId(GetObjectType());
    auto* clonedTrunkNode = factory->InstantiateNode(clonedId, externalCellTag);

    *needCustomEndCopy = LoadInplace(clonedTrunkNode, context, factory);

    return clonedTrunkNode;
}

void TNontemplateCypressNodeTypeHandlerBase::EndCopyInplaceCore(
    TCypressNode* trunkNode,
    TEndCopyContext* context,
    ICypressNodeFactory* factory,
    TNodeId sourceNodeId)
{
    // See BeginCopyCore.
    auto externalCellTag = Load<TCellTag>(*context);
    if (externalCellTag != trunkNode->GetExternalCellTag()) {
        THROW_ERROR_EXCEPTION("Cannot inplace copy node %v to node %v since external cell tags do not match: %v != %v",
            sourceNodeId,
            trunkNode->GetId(),
            externalCellTag,
            trunkNode->GetExternalCellTag());
    }

    LoadInplace(trunkNode, context, factory);
}

bool TNontemplateCypressNodeTypeHandlerBase::LoadInplace(
    TCypressNode* trunkNode,
    TEndCopyContext* context,
    ICypressNodeFactory* factory)
{
    auto* sourceAccount = Load<TAccount*>(*context);
    auto sourceResourceUsage = Load<TClusterResources>(*context);

    auto* clonedAccount = factory->GetClonedNodeAccount(sourceAccount);
    factory->ValidateClonedAccount(
        context->GetMode(),
        sourceAccount,
        sourceResourceUsage,
        clonedAccount);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->SetAccount(
        trunkNode,
        clonedAccount,
        /* transaction */ nullptr);

    auto sourceAcd = Load<TAccessControlDescriptor>(*context);
    if ((context->GetMode() == ENodeCloneMode::Move || factory->ShouldPreserveAcl()) &&
        trunkNode->GetType() != EObjectType::PortalExit)
    {
        trunkNode->Acd().SetInherit(sourceAcd.GetInherit());
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
    auto opaque = Load<bool>(*context);
    trunkNode->SetOpaque(opaque);

    // Copy annotation.
    auto optionalAnnotation = Load<std::optional<TString>>(*context);
    if (optionalAnnotation) {
        trunkNode->SetAnnotation(*optionalAnnotation);
    } else {
        trunkNode->RemoveAnnotation();
    }

    // Copy creation time.
    auto creationTime = Load<TInstant>(*context);
    if (factory->ShouldPreserveCreationTime()) {
        trunkNode->SetCreationTime(creationTime);
    }

    // Copy modification time.
    auto modificationTime = Load<TInstant>(*context);
    if (factory->ShouldPreserveModificationTime()) {
        trunkNode->SetModificationTime(modificationTime);
    }

    // Copy expiration time.
    auto expirationTime = Load<std::optional<TInstant>>(*context);
    if (factory->ShouldPreserveExpirationTime() && expirationTime) {
        trunkNode->SetExpirationTime(*expirationTime);
    }

    // Copy expiration timeout.
    auto expirationTimeout = Load<std::optional<TDuration>>(*context);
    if (factory->ShouldPreserveExpirationTimeout() && expirationTimeout) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetExpirationTimeout(trunkNode, expirationTimeout);
    }

    // Copy attributes directly to suppress validation.
    auto keyToAttribute = Load<std::vector<std::pair<TString, TYsonString>>>(*context);
    if (!keyToAttribute.empty()) {
        auto* clonedAttributes = trunkNode->GetMutableAttributes();
        const auto& ysonInternRegistry = Bootstrap_->GetYsonInternRegistry();
        for (const auto& [key, value] : keyToAttribute) {
            // NB: overwriting already existing attributes is essential in the inplace case.
            clonedAttributes->Set(key, ysonInternRegistry->Intern(value));
        }
    }

    auto opaqueChild = Load<bool>(*context);
    return !opaqueChild;
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
        /* transaction */ nullptr);

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
    if (mode == ENodeCloneMode::Move || factory->ShouldPreserveAcl()) {
        clonedTrunkNode->Acd().SetInherit(sourceNode->Acd().GetInherit());
        for (const auto& ace : sourceNode->Acd().Acl().Entries) {
            clonedTrunkNode->Acd().AddEntry(ace);
        }
    }

    // Copy builtin attributes.
    clonedTrunkNode->SetOpaque(sourceNode->GetOpaque());
    if (auto optionalAnnotation = sourceNode->TryGetAnnotation()) {
        clonedTrunkNode->SetAnnotation(*optionalAnnotation);
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(clonedTrunkNode);
}

////////////////////////////////////////////////////////////////////////////////

template <bool Transient>
void TCompositeNodeBase::TAttributes<Transient>::Persist(const NCellMaster::TPersistenceContext& context)
    requires (!Transient)
{
    using NCellMaster::EMasterReign;
    using NYT::Persist;

    Persist(context, CompressionCodec);
    Persist(context, ErasureCodec);
    Persist(context, HunkErasureCodec);
    Persist(context, EnableStripedErasure);
    Persist(context, ReplicationFactor);
    Persist(context, Vital);
    Persist(context, Atomicity);
    Persist(context, CommitOrdering);
    Persist(context, InMemoryMode);
    Persist(context, OptimizeFor);
    Persist(context, ProfilingMode);
    Persist(context, ProfilingTag);
    Persist(context, ChunkMergerMode);
    Persist(context, PrimaryMediumIndex);
    Persist(context, Media);
    Persist(context, TabletCellBundle);
    Persist(context, ChaosCellBundle);
}

template <bool Transient>
void TCompositeNodeBase::TAttributes<Transient>::Persist(const NCypressServer::TCopyPersistenceContext& context)
    requires (!Transient)
{
    using NYT::Persist;
#define XX(camelCaseName, snakeCaseName) \
    Persist(context, camelCaseName);

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeNodeBase::TAttributes<Transient>::AreFull() const
{
#define XX(camelCaseName, snakeCaseName) \
    && camelCaseName.IsSet()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
bool TCompositeNodeBase::TAttributes<Transient>::AreEmpty() const
{
#define XX(camelCaseName, snakeCaseName) \
    && !camelCaseName.IsNull()
    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);
#undef XX
}

template <bool Transient>
TCompositeNodeBase::TPersistentAttributes TCompositeNodeBase::TAttributes<Transient>::ToPersistent() const requires Transient
{
    TPersistentAttributes result;
#define XX(camelCaseName, snakeCaseName) \
    if (camelCaseName.IsSet()) { \
        result.camelCaseName.Set(TVersionedBuiltinAttributeTraits<decltype(result.camelCaseName)::TValue>::FromRaw(camelCaseName.Unbox())); \
    }
    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
    return result;
}

template struct TCompositeNodeBase::TAttributes<true>;
template struct TCompositeNodeBase::TAttributes<false>;

////////////////////////////////////////////////////////////////////////////////

void TCompositeNodeBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNode::Save(context);

    using NYT::Save;
    TUniquePtrSerializer<>::Save(context, Attributes_);
}

void TCompositeNodeBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNode::Load(context);

    using NYT::Load;
    TUniquePtrSerializer<>::Load(context, Attributes_);
}

bool TCompositeNodeBase::HasInheritableAttributes() const
{
    for (auto* node = this; node; node = node->GetOriginator()->As<TCompositeNodeBase>()) {
        if (node->Attributes_) {
            YT_ASSERT(!node->Attributes_->AreEmpty());
            return true;
        }
    }

    return false;
}

const TCompositeNodeBase::TPersistentAttributes* TCompositeNodeBase::FindAttributes() const
{
    return Attributes_.get();
}

void TCompositeNodeBase::FillInheritableAttributes(TTransientAttributes* attributes) const
{
#define XX(camelCaseName, snakeCaseName) \
    if (!attributes->camelCaseName.IsSet()) { \
        if (auto inheritedValue = TryGet##camelCaseName()) { \
            attributes->camelCaseName.Set(*inheritedValue); \
        } \
    }

    if (HasInheritableAttributes()) {
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
    }
#undef XX
}

void TCompositeNodeBase::SetAttributes(const TPersistentAttributes* attributes)
{
    if (!attributes || attributes->AreEmpty()) {
        Attributes_.reset();
    } else if (Attributes_) {
        *Attributes_ = *attributes;
    } else {
        Attributes_ = std::make_unique<TPersistentAttributes>(*attributes);
    }
}

void TCompositeNodeBase::CloneAttributesFrom(const TCompositeNodeBase* sourceNode)
{
    auto* attributes = sourceNode->FindAttributes();
    SetAttributes(attributes);
}

void TCompositeNodeBase::MergeAttributesFrom(const TCompositeNodeBase* branchedNode)
{
    auto* attributes = branchedNode->FindAttributes();
    if (!attributes) {
        return;
    }

    if (!Attributes_) {
        SetAttributes(attributes);
        return;
    }

#define XX(camelCaseName, snakeCaseName) \
    Attributes_->camelCaseName.Merge(attributes->camelCaseName, IsTrunk());

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX
}

#define XX(camelCaseName, snakeCaseName) \
const decltype(std::declval<TCompositeNodeBase::TPersistentAttributes>().camelCaseName)* TCompositeNodeBase::DoTryGet##camelCaseName() const \
{ \
    return Attributes_ ? &Attributes_->camelCaseName : nullptr; \
} \
\
auto TCompositeNodeBase::TryGet##camelCaseName() const -> std::optional<TRawVersionedBuiltinAttributeType<T##camelCaseName>> \
{ \
    using TAttribute = decltype(TPersistentAttributes::camelCaseName); \
    return TAttribute::TryGet(&TCompositeNodeBase::DoTryGet##camelCaseName, this); \
} \
\
bool TCompositeNodeBase::Has##camelCaseName() const \
{ \
    return TryGet##camelCaseName().has_value(); \
} \
\
void TCompositeNodeBase::Set##camelCaseName(TCompositeNodeBase::T##camelCaseName value) \
{ \
    if (!Attributes_) { \
        Attributes_ = std::make_unique<TPersistentAttributes>(); \
    } \
    Attributes_->camelCaseName.Set(std::move(value)); \
} \
\
void TCompositeNodeBase::Remove##camelCaseName() \
{ \
    if (!Attributes_) { \
        return; \
    } \
\
    if (IsTrunk()) { \
        Attributes_->camelCaseName.Reset(); \
    } else { \
        Attributes_->camelCaseName.Remove(); \
    } \
\
    if (Attributes_->AreEmpty()) { \
        Attributes_.reset(); \
    } \
}

FOR_EACH_INHERITABLE_ATTRIBUTE(XX)

////////////////////////////////////////////////////////////////////////////////

void GatherInheritableAttributes(TCypressNode* node, TCompositeNodeBase::TTransientAttributes* attributes)
{
    for (auto* ancestor = node; ancestor && !attributes->AreFull(); ancestor = ancestor->GetParent()) {
        ancestor->As<TCompositeNodeBase>()->FillInheritableAttributes(attributes);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
void TCompositeNodeTypeHandler<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

    clonedTrunkNode->CloneAttributesFrom(sourceNode);
}

template <class TImpl>
void TCompositeNodeTypeHandler<TImpl>::DoBranch(
    const TImpl* originatingNode,
    TImpl* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    // NB: leaving branch's Attributes_ null here.
}

template <class TImpl>
void TCompositeNodeTypeHandler<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    originatingNode->MergeAttributesFrom(branchedNode);
}

template <class TImpl>
bool TCompositeNodeTypeHandler<TImpl>::HasBranchedChangesImpl(
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
void TCompositeNodeTypeHandler<TImpl>::DoBeginCopy(
    TImpl* node,
    TBeginCopyContext* context)
{
    TBase::DoBeginCopy(node, context);

    using NYT::Save;
    const auto* attributes = node->FindAttributes();
    Save(*context, attributes != nullptr);
    if (attributes) {
        Save(*context, *attributes);
    }
}

template <class TImpl>
void TCompositeNodeTypeHandler<TImpl>::DoEndCopy(
    TImpl* trunkNode,
    TEndCopyContext* context,
    ICypressNodeFactory* factory)
{
    TBase::DoEndCopy(trunkNode, context, factory);

    using NYT::Load;
    if (Load<bool>(*context)) {
        auto attributes = Load<TCompositeNodeBase::TPersistentAttributes>(*context);
        trunkNode->SetAttributes(&attributes);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMapNodeChildren::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, KeyToChild_);
}

void TMapNodeChildren::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, KeyToChild_);

    // Reconstruct ChildToKey map.
    for (const auto& [key, childNode] : KeyToChild_) {
        if (childNode) {
            EmplaceOrCrash(ChildToKey_, TCypressNodePtr(childNode, TObjectPtrLoadTag()), key);
        }
    }

    RecomputeMasterMemoryUsage();
}

void TMapNodeChildren::RecomputeMasterMemoryUsage()
{
    MasterMemoryUsage_ = 0;
    for (const auto& [key, childNode] : KeyToChild_) {
        MasterMemoryUsage_ += std::ssize(key);
    }
}

void TMapNodeChildren::Set(const TString& key, TCypressNode* child)
{
    YT_VERIFY(!child || child->IsTrunk());

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

void TMapNodeChildren::Insert(const TString& key, TCypressNode* child)
{
    YT_VERIFY(!child || child->IsTrunk());

    YT_VERIFY(KeyToChild_.emplace(key, child).second);
    MasterMemoryUsage_ += std::ssize(key);

    if (child) {
        EmplaceOrCrash(ChildToKey_, child, key);
    }
}

void TMapNodeChildren::Remove(const TString& key, TCypressNode* child)
{
    YT_VERIFY(!child || child->IsTrunk());

    auto it = KeyToChild_.find(key);
    YT_VERIFY(it != KeyToChild_.end());
    YT_VERIFY(it->second == child);
    MasterMemoryUsage_ -= std::ssize(key);
    KeyToChild_.erase(it);
    if (child) {
        EraseOrCrash(ChildToKey_, child);
    }
}

bool TMapNodeChildren::Contains(const TString& key) const
{
    return KeyToChild_.find(key) != KeyToChild_.end();
}

const TMapNodeChildren::TKeyToChild& TMapNodeChildren::KeyToChild() const
{
    return KeyToChild_;
}

const TMapNodeChildren::TChildToKey& TMapNodeChildren::ChildToKey() const
{
    return ChildToKey_;
}

int TMapNodeChildren::GetRefCount() const noexcept
{
    return RefCount_;
}

void TMapNodeChildren::Ref() noexcept
{
    ++RefCount_;
}

void TMapNodeChildren::Unref() noexcept
{
    YT_VERIFY(--RefCount_ >= 0);
}

/*static*/ std::unique_ptr<TMapNodeChildren> TMapNodeChildren::Copy(TMapNodeChildren* srcChildren)
{
    YT_VERIFY(srcChildren->GetRefCount() != 0);

    auto dstChildren = std::make_unique<TMapNodeChildren>();

    dstChildren->KeyToChild_ = srcChildren->KeyToChild_;
    // NB: the order of refs here is non-deterministic but this should not be a problem.
    dstChildren->ChildToKey_ = srcChildren->ChildToKey_;

    dstChildren->RecomputeMasterMemoryUsage();

    return dstChildren;
}

////////////////////////////////////////////////////////////////////////////////

const TMapNode::TKeyToChild& TMapNode::KeyToChild() const
{
    return Children_.Get().KeyToChild();
}

const TMapNode::TChildToKey& TMapNode::ChildToKey() const
{
    return Children_.Get().ChildToKey();
}

TMapNodeChildren& TMapNode::MutableChildren()
{
    return Children_.MutableGet();
}

ENodeType TMapNode::GetNodeType() const
{
    return ENodeType::Map;
}

void TMapNode::Save(NCellMaster::TSaveContext& context) const
{
    TCompositeNodeBase::Save(context);

    using NYT::Save;
    Save(context, ChildCountDelta_);
    Save(context, Children_);
}

void TMapNode::Load(NCellMaster::TLoadContext& context)
{
    TCompositeNodeBase::Load(context);

    using NYT::Load;

    Load(context, ChildCountDelta_);
    Load(context, Children_);
}

int TMapNode::GetGCWeight() const
{
    return TObject::GetGCWeight() + KeyToChild().size();
}

TDetailedMasterMemory TMapNode::GetDetailedMasterMemoryUsage() const
{
    auto result = TCompositeNodeBase::GetDetailedMasterMemoryUsage();
    result[EMasterMemoryType::Nodes] += Children_.Get().GetMasterMemoryUsage();
    return result;
}

void TMapNode::AssignChildren(const TObjectPartCoWPtr<TMapNodeChildren>& children)
{
    Children_.Assign(children);
    MutableChildren().RecomputeMasterMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
EObjectType TMapNodeTypeHandlerImpl<TImpl>::GetObjectType() const
{
    return EObjectType::MapNode;
}

template <class TImpl>
ENodeType TMapNodeTypeHandlerImpl<TImpl>::GetNodeType() const
{
    return ENodeType::Map;
}

template <class TImpl>
void TMapNodeTypeHandlerImpl<TImpl>::DoDestroy(TImpl* node)
{
    node->ChildCountDelta_ = 0;
    node->Children_.Reset();

    TBase::DoDestroy(node);
}

template <class TImpl>
void TMapNodeTypeHandlerImpl<TImpl>::DoBranch(
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
            const auto& cypressManager = this->Bootstrap_->GetCypressManager();

            THashMap<TString, TCypressNode*> keyToChildStorage;
            const auto& originatingNodeChildren = GetMapNodeChildMap(
                cypressManager,
                originatingNode->GetTrunkNode()->template As<TMapNode>(),
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
void TMapNodeTypeHandlerImpl<TImpl>::DoMerge(
    TImpl* originatingNode,
    TImpl* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    bool isOriginatingNodeBranched = originatingNode->GetTransaction() != nullptr;

    auto& children = originatingNode->MutableChildren();
    const auto& keyToChild = originatingNode->KeyToChild();

    for (const auto& [key, trunkChildNode] : SortHashMapByKeys(branchedNode->KeyToChild())) {
        auto it = keyToChild.find(key);
        if (trunkChildNode) {
            children.Set(key, trunkChildNode);
        } else {
            // Branched: tombstone
            if (it == keyToChild.end()) {
                // Originating: missing
                if (isOriginatingNodeBranched) {
                    children.Insert(key, nullptr);
                }
            } else if (it->second) {
                // Originating: present
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
ICypressNodeProxyPtr TMapNodeTypeHandlerImpl<TImpl>::DoGetProxy(
    TImpl* trunkNode,
    TTransaction* transaction)
{
    return New<TMapNodeProxy>(
        this->Bootstrap_,
        &this->Metadata_,
        transaction,
        trunkNode);
}

template <class TImpl>
void TMapNodeTypeHandlerImpl<TImpl>::DoClone(
    TImpl* sourceNode,
    TImpl* clonedTrunkNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

    auto* transaction = factory->GetTransaction();

    const auto& cypressManager = this->Bootstrap_->GetCypressManager();

    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        sourceNode->GetTrunkNode()->template As<TMapNode>(),
        transaction,
        &keyToChildMapStorage);
    auto keyToChildList = SortHashMapByKeys(keyToChildMap);

    auto& clonedChildren = clonedTrunkNode->MutableChildren();

    for (const auto& [key, trunkChildNode] : keyToChildList) {
        auto* childNode = cypressManager->GetVersionedNode(trunkChildNode, transaction);

        auto* clonedChildNode = factory->CloneNode(childNode, mode);
        auto* clonedTrunkChildNode = clonedChildNode->GetTrunkNode();

        clonedChildren.Insert(key, clonedTrunkChildNode);

        AttachChild(clonedTrunkNode, clonedChildNode);

        ++clonedTrunkNode->ChildCountDelta();
    }
}

template <class TImpl>
bool TMapNodeTypeHandlerImpl<TImpl>::HasBranchedChangesImpl(TImpl* originatingNode, TImpl* branchedNode)
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
void TMapNodeTypeHandlerImpl<TImpl>::DoBeginCopy(
    TImpl* node,
    TBeginCopyContext* context)
{
    TBase::DoBeginCopy(node, context);

    using NYT::Save;

    const auto& cypressManager = this->Bootstrap_->GetCypressManager();

    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        node->GetTrunkNode()->template As<TImpl>(),
        node->GetTransaction(),
        &keyToChildMapStorage);

    TSizeSerializer::Save(*context, keyToChildMap.size());
    for (const auto& [key, child] : SortHashMapByKeys(keyToChildMap)) {
        Save(*context, key);
        const auto& typeHandler = cypressManager->GetHandler(child);
        typeHandler->BeginCopy(child, context);
    }
}

template <class TImpl>
void TMapNodeTypeHandlerImpl<TImpl>::DoEndCopy(
    TImpl* trunkNode,
    TEndCopyContext* context,
    ICypressNodeFactory* factory)
{
    TBase::DoEndCopy(trunkNode, context, factory);

    using NYT::Load;

    auto& children = trunkNode->MutableChildren();

    size_t size = TSizeSerializer::Load(*context);
    for (size_t index = 0; index < size; ++index) {
        auto key = Load<TString>(*context);

        auto* childNode = factory->EndCopyNode(context);
        auto* trunkChildNode = childNode->GetTrunkNode();

        children.Insert(key, trunkChildNode);

        AttachChild(trunkNode->GetTrunkNode(), childNode);

        ++trunkNode->ChildCountDelta();
    }
}

// Explicit instantiations.
template class TMapNodeTypeHandlerImpl<TMapNode>;
template class TMapNodeTypeHandlerImpl<TPortalExitNode>;
template class TMapNodeTypeHandlerImpl<TScionNode>;

////////////////////////////////////////////////////////////////////////////////

ENodeType TListNode::GetNodeType() const
{
    return ENodeType::List;
}

void TListNode::Save(NCellMaster::TSaveContext& context) const
{
    TCompositeNodeBase::Save(context);

    using NYT::Save;
    TVectorSerializer<
        TRawNonversionedObjectPtrSerializer
    >::Save(context, IndexToChild_);
}

void TListNode::Load(NCellMaster::TLoadContext& context)
{
    TCompositeNodeBase::Load(context);

    using NYT::Load;
    TVectorSerializer<
        TRawNonversionedObjectPtrSerializer
    >::Load(context, IndexToChild_);

    // Reconstruct ChildToIndex.
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

std::unique_ptr<TCypressNode> TListNodeTypeHandler::Instantiate(
    TVersionedNodeId id,
    NObjectClient::TCellTag externalCellTag)
{
    THROW_ERROR_EXCEPTION_IF(
        GetDynamicCypressManagerConfig()->ForbidListNodeCreation,
        "List nodes are deprecated and will be removed in the near future");

    return TBase::Instantiate(id, externalCellTag);
}

ICypressNodeProxyPtr TListNodeTypeHandler::DoGetProxy(
    TListNode* trunkNode,
    TTransaction* transaction)
{
    return New<TListNodeProxy>(
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

void TListNodeTypeHandler::DoDestroy(TListNode* node)
{
    // Drop references to the children.
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* child : node->IndexToChild()) {
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
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* child : originatingNode->IndexToChild()) {
        objectManager->RefObject(child);
    }
}

void TListNodeTypeHandler::DoMerge(
    TListNode* originatingNode,
    TListNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    // Drop all references held by the originator.
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* child : originatingNode->IndexToChild()) {
        objectManager->UnrefObject(child);
    }

    // Replace the child list with the branched copy.
    originatingNode->IndexToChild().swap(branchedNode->IndexToChild());
    originatingNode->ChildToIndex().swap(branchedNode->ChildToIndex());
}

void TListNodeTypeHandler::DoClone(
    TListNode* sourceNode,
    TListNode* clonedTrunkNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedTrunkNode, factory, mode, account);

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& indexToChild = sourceNode->IndexToChild();

    for (int index = 0; index < static_cast<int>(indexToChild.size()); ++index) {
        auto* childNode = indexToChild[index];
        auto* clonedChildNode = factory->CloneNode(childNode, mode);
        auto* clonedChildTrunkNode = clonedChildNode->GetTrunkNode();

        clonedTrunkNode->IndexToChild().push_back(clonedChildTrunkNode);
        YT_VERIFY(clonedTrunkNode->ChildToIndex().emplace(clonedChildTrunkNode, index).second);

        AttachChild(clonedTrunkNode, clonedChildNode);
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

void TListNodeTypeHandler::DoBeginCopy(
    TListNode* node,
    TBeginCopyContext* context)
{
    TBase::DoBeginCopy(node, context);

    using NYT::Save;

    const auto& cypressManager = Bootstrap_->GetCypressManager();

    const auto& children = node->IndexToChild();
    TSizeSerializer::Save(*context, children.size());
    for (auto* child : children) {
        const auto& typeHandler = cypressManager->GetHandler(child);
        typeHandler->BeginCopy(child, context);
    }
}

void TListNodeTypeHandler::DoEndCopy(
    TListNode* trunkNode,
    TEndCopyContext* context,
    ICypressNodeFactory* factory)
{
    TBase::DoEndCopy(trunkNode, context, factory);

    using NYT::Load;

    const auto& objectManager = this->Bootstrap_->GetObjectManager();
    auto& indexToChild = trunkNode->IndexToChild();
    auto& childToIndex = trunkNode->ChildToIndex();

    int size = static_cast<int>(TSizeSerializer::Load(*context));
    for (int index = 0; index < size; ++index) {
        auto* childNode = factory->EndCopyNode(context);
        auto* trunkChildNode = childNode->GetTrunkNode();

        indexToChild.push_back(trunkChildNode);
        YT_VERIFY(childToIndex.emplace(trunkChildNode, index).second);

        AttachChild(trunkNode, childNode);
        objectManager->RefObject(childNode->GetTrunkNode());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

