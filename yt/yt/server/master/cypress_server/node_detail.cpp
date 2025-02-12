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

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/maintenance_tracker_server/cluster_proxy_node.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>

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

static constexpr auto& Logger = CypressServerLogger;

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

void TNontemplateCypressNodeTypeHandlerBase::SerializeNodeCore(
    TCypressNode* node,
    TSerializeNodeContext* context)
{
    using NYT::Save;

    auto erasedType = node->GetType();
    if (erasedType == EObjectType::PortalExit) {
        erasedType = EObjectType::MapNode;
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
        auto clonedId = objectManager->GenerateId(GetObjectType());
        trunkNode = factory->InstantiateNode(clonedId, context->GetExternalCellTag());
    }

    auto sourceAccount = Load<TAccountRawPtr>(*context);
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
        /*transaction*/ nullptr);

    auto sourceAcd = Load<TAccessControlDescriptor>(*context);
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
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetExpirationTime(trunkNode, expirationTime);
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

    if (originatingNode->IsSequoia() && originatingNode->IsNative()) {
        if (branchedNode->MutableSequoiaProperties()->Tombstone) {
            if (originatingNode->IsTrunk()) {
                const auto& objectManager = Bootstrap_->GetObjectManager();
                objectManager->UnrefObject(originatingNode);
            } else {
                originatingNode->MutableSequoiaProperties()->Tombstone = true;
            }
        }
    }
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
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Persist(context, OptimizeFor);
    } else {
        auto compatOptimizeFor = Load<TVersionedBuiltinAttribute<NTableClient::ECompatOptimizeFor>>(context.LoadContext());
        if (compatOptimizeFor.IsNull()) {
            OptimizeFor.Reset();
        } else if (compatOptimizeFor.IsTombstoned()) {
            OptimizeFor.Remove();
        } else if (compatOptimizeFor.IsSet()) {
            auto optimizeFor = compatOptimizeFor.ToOptional();
            YT_VERIFY(optimizeFor);
            OptimizeFor.Set(CheckedEnumCast<NTableClient::EOptimizeFor>(*optimizeFor));
        }
    }
    Persist(context, ProfilingMode);
    Persist(context, ProfilingTag);
    // COMPAT(cherepashka)
    if (context.GetVersion() >= EMasterReign::EnumsAndChunkReplicationReductionsInTTableNode) {
        Persist(context, ChunkMergerMode);
    } else {
        auto compatChunkMergerMode = Load<TVersionedBuiltinAttribute<NChunkClient::ECompatChunkMergerMode>>(context.LoadContext());
        if (compatChunkMergerMode.IsNull()) {
            ChunkMergerMode.Reset();
        } else if (compatChunkMergerMode.IsTombstoned()) {
            ChunkMergerMode.Remove();
        } else if (compatChunkMergerMode.IsSet()) {
            auto chunkMergerMode = compatChunkMergerMode.ToOptional();
            YT_VERIFY(chunkMergerMode);
            ChunkMergerMode.Set(CheckedEnumCast<NChunkClient::EChunkMergerMode>(*chunkMergerMode));
        }
    }
    Persist(context, PrimaryMediumIndex);
    Persist(context, Media);
    Persist(context, TabletCellBundle);
    Persist(context, ChaosCellBundle);

    // COMPAT(kivedernikov)
    if (!context.IsLoad() || context.GetVersion() >= EMasterReign::HunkMedia) {
        Persist(context, HunkMedia);
        Persist(context, HunkPrimaryMediumIndex);
    }
}

template <bool Transient>
void TCompositeNodeBase::TAttributes<Transient>::Persist(const NCypressServer::TCopyPersistenceContext& context)
    requires Transient
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
TCompositeNodeBase::TPersistentAttributes
TCompositeNodeBase::TAttributes<Transient>::ToPersistent() const
    requires Transient
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

template <bool Transient>
TCompositeNodeBase::TAttributes<Transient>
TCompositeNodeBase::TAttributes<Transient>::Clone() const
    requires (!Transient)
{
    TCompositeNodeBase::TAttributes<Transient> result;
#define XX(camelCaseName, snakeCaseName) \
    result.camelCaseName = camelCaseName.Clone();
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

bool TCompositeNodeBase::CompareInheritableAttributes(
    const TTransientAttributes& attributes,
    ENodeMaterializationReason reason) const
{
    if (!HasInheritableAttributes()) {
        return attributes.AreEmpty();
    }

#define XX(camelCaseName, snakeCaseName) \
    if (TryGet##camelCaseName() != attributes.camelCaseName.ToOptional()) { \
        return false; \
    }

    if (reason == ENodeMaterializationReason::Copy) {
        FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
    } else {
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
    }
#undef XX

    return true;
}

TConstInheritedAttributeDictionaryPtr TCompositeNodeBase::MaybePatchInheritableAttributes(
    const TConstInheritedAttributeDictionaryPtr& attributes,
    ENodeMaterializationReason reason) const
{
    if (CompareInheritableAttributes(attributes->Attributes())) {
        return attributes;
    }

    auto updatedInheritedAttributeDictionary = New<TInheritedAttributeDictionary>(attributes);
    auto& underlyingAttributes = updatedInheritedAttributeDictionary->MutableAttributes();

#define XX(camelCaseName, snakeCaseName) \
    if (auto inheritedValue = TryGet##camelCaseName()) { \
        underlyingAttributes.camelCaseName.Set(*inheritedValue); \
    }

    if (reason == ENodeMaterializationReason::Copy) {
        FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
    } else {
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
    }
#undef XX

    return updatedInheritedAttributeDictionary;
}

void TCompositeNodeBase::FillInheritableAttributes(TTransientAttributes* attributes, ENodeMaterializationReason reason) const
{
#define XX(camelCaseName, snakeCaseName) \
    if (!attributes->camelCaseName.IsSet()) { \
        if (auto inheritedValue = TryGet##camelCaseName()) { \
            attributes->camelCaseName.Set(*inheritedValue); \
        } \
    }

    if (HasInheritableAttributes()) {
        if (reason == ENodeMaterializationReason::Copy) {
            FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
        } else {
            FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
        }
    }
#undef XX
}

void TCompositeNodeBase::SetAttributes(const TPersistentAttributes* attributes)
{
    if (!attributes || attributes->AreEmpty()) {
        Attributes_.reset();
    } else if (Attributes_) {
        *Attributes_ = attributes->Clone();
    } else {
        Attributes_ = std::make_unique<TPersistentAttributes>(attributes->Clone());
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
#undef XX

////////////////////////////////////////////////////////////////////////////////

void GatherInheritableAttributes(
    const TCypressNode* node,
    TCompositeNodeBase::TTransientAttributes* attributes,
    ENodeMaterializationReason reason)
{
    for (auto* ancestor = node; ancestor && !attributes->AreFull(); ancestor = ancestor->GetParent()) {
        ancestor->As<TCompositeNodeBase>()->FillInheritableAttributes(attributes, reason);
    }
}

////////////////////////////////////////////////////////////////////////////////

TInheritedAttributeDictionary::TInheritedAttributeDictionary(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

TInheritedAttributeDictionary::TInheritedAttributeDictionary(
    const TBootstrap* bootstrap,
    NYTree::IAttributeDictionaryPtr&& attributes)
    : Bootstrap_(bootstrap)
{
    for (const auto& [key, value] : attributes->ListPairs()){
        SetYson(key, value);
    }
}

TInheritedAttributeDictionary::TInheritedAttributeDictionary(const TConstInheritedAttributeDictionaryPtr& other)
    : TInheritedAttributeDictionary(
        other->Bootstrap_,
        other->Clone())
{ }

auto TInheritedAttributeDictionary::ListKeys() const -> std::vector<TKey>
{
    std::vector<TKey> result;
#define XX(camelCaseName, snakeCaseName) \
    if (InheritedAttributes_.camelCaseName.IsSet()) {  \
        result.push_back(#snakeCaseName); \
    }

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (Fallback_) {
        auto fallbackKeys = Fallback_->ListKeys();
        result.insert(result.end(), fallbackKeys.begin(), fallbackKeys.end());
        SortUnique(result);
    }

    return result;
}

auto TInheritedAttributeDictionary::ListPairs() const -> std::vector<TKeyValuePair>
{
    return ListAttributesPairs(*this);
}

auto TInheritedAttributeDictionary::FindYson(TKeyView key) const -> TValue
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        auto optionalValue = InheritedAttributes_.camelCaseName.ToOptional(); \
        return optionalValue ? ConvertToYsonString(*optionalValue) : TYsonString(); \
    } \

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
#undef XX

    if (key == EInternedAttributeKey::PrimaryMedium.Unintern()) {
        auto optionalPrimaryMediumIndex = InheritedAttributes_.PrimaryMediumIndex.ToOptional();
        if (!optionalPrimaryMediumIndex) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*optionalPrimaryMediumIndex);
        return ConvertToYsonString(medium->GetName());
    }

    if (key == EInternedAttributeKey::HunkPrimaryMedium.Unintern()) {
        auto optionalHunkPrimaryMediumIndex = InheritedAttributes_.HunkPrimaryMediumIndex.ToOptional();
        if (!optionalHunkPrimaryMediumIndex) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*optionalHunkPrimaryMediumIndex);
        return ConvertToYsonString(medium->GetName());
    }

    if (key == EInternedAttributeKey::Media.Unintern()) {
        auto optionalReplication = InheritedAttributes_.Media.ToOptional();
        if (!optionalReplication) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return ConvertToYsonString(TSerializableChunkReplication(*optionalReplication, chunkManager));
    }

    if (key == EInternedAttributeKey::HunkMedia.Unintern()) {
        auto optionalReplication = InheritedAttributes_.HunkMedia.ToOptional();
        if (!optionalReplication) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return ConvertToYsonString(TSerializableChunkReplication(*optionalReplication, chunkManager));
    }

    if (key == EInternedAttributeKey::TabletCellBundle.Unintern()) {
        auto optionalCellBundle = InheritedAttributes_.TabletCellBundle.ToOptional();
        if (!optionalCellBundle) {
            return {};
        }
        YT_VERIFY(*optionalCellBundle);
        return ConvertToYsonString((*optionalCellBundle)->GetName());
    }

    if (key == EInternedAttributeKey::ChaosCellBundle.Unintern()) {
        auto optionalCellBundle = InheritedAttributes_.ChaosCellBundle.ToOptional();
        if (!optionalCellBundle) {
            return {};
        }
        YT_VERIFY(*optionalCellBundle);
        return ConvertToYsonString((*optionalCellBundle)->GetName());
    }

    return Fallback_ ? Fallback_->FindYson(key) : TYsonString();
}

void TInheritedAttributeDictionary::SetYson(TKeyView key, const TYsonString& value)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        if (key == EInternedAttributeKey::CompressionCodec.Unintern()) { \
            const auto& chunkManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager; \
            ValidateCompressionCodec( \
                value, \
                chunkManagerConfig->ForbiddenCompressionCodecs, \
                chunkManagerConfig->ForbiddenCompressionCodecNameToAlias); \
        } \
        if (key == EInternedAttributeKey::ErasureCodec.Unintern()) { \
            ValidateErasureCodec( \
                value, \
                Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ForbiddenErasureCodecs); \
        } \
        using TAttr = decltype(InheritedAttributes_.camelCaseName)::TValue; \
        InheritedAttributes_.camelCaseName.Set(ConvertTo<TAttr>(value)); \
        return; \
    }

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (key == EInternedAttributeKey::PrimaryMedium.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mediumName = ConvertTo<std::string>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        InheritedAttributes_.PrimaryMediumIndex.Set(medium->GetIndex());
        return;
    }

    if (key == EInternedAttributeKey::HunkPrimaryMedium.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mediumName = ConvertTo<std::string>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        InheritedAttributes_.HunkPrimaryMediumIndex.Set(medium->GetIndex());
        return;
    }

    if (key == EInternedAttributeKey::Media.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        InheritedAttributes_.Media.Set(replication);
        return;
    }

    if (key == EInternedAttributeKey::HunkMedia.Unintern()) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        InheritedAttributes_.HunkMedia.Set(replication);
        return;
    }

    if (key == EInternedAttributeKey::TabletCellBundle.Unintern()) {
        auto bundleName = ConvertTo<std::string>(value);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* bundle = tabletManager->GetTabletCellBundleByNameOrThrow(bundleName, true /*activeLifeStageOnly*/);
        InheritedAttributes_.TabletCellBundle.Set(bundle);
        return;
    }

    if (key == EInternedAttributeKey::ChaosCellBundle.Unintern()) {
        auto bundleName = ConvertTo<std::string>(value);
        const auto& chaosManager = Bootstrap_->GetChaosManager();
        auto* bundle = chaosManager->GetChaosCellBundleByNameOrThrow(bundleName, true /*activeLifeStageOnly*/);
        InheritedAttributes_.ChaosCellBundle.Set(bundle);
        return;
    }

    if (!Fallback_) {
        Fallback_ = CreateEphemeralAttributes();
    }

    Fallback_->SetYson(key, value);
}

bool TInheritedAttributeDictionary::Remove(TKeyView key)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        InheritedAttributes_.camelCaseName.Reset(); \
        return true; \
    }

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (Fallback_) {
        return Fallback_->Remove(key);
    }

    return false;
}

TCompositeNodeBase::TTransientAttributes& TInheritedAttributeDictionary::MutableAttributes()
{
    return InheritedAttributes_;
}

const TCompositeNodeBase::TTransientAttributes& TInheritedAttributeDictionary::Attributes() const
{
    return InheritedAttributes_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
void TCompositeNodeTypeHandler<TImpl>::DoClone(
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
void TCompositeNodeTypeHandler<TImpl>::DoSerializeNode(
    TImpl* node,
    TSerializeNodeContext* context)
{
    TBase::DoSerializeNode(node, context);

    using NYT::Save;
    TCompositeNodeBase::TTransientAttributes attributes;
    // NB: Using ENodeMaterializationReason::Create here, since we need a full list of attributes for each node.
    node->FillInheritableAttributes(&attributes, ENodeMaterializationReason::Create);
    Save(*context, !attributes.AreEmpty());
    if (!attributes.AreEmpty()) {
        Save(*context, attributes);
    }
}

template <class TImpl>
void TCompositeNodeTypeHandler<TImpl>::DoMaterializeNode(
    TImpl* trunkNode,
    TMaterializeNodeContext* context)
{
    TBase::DoMaterializeNode(trunkNode, context);

    using NYT::Load;
    if (Load<bool>(*context)) {
        auto attributes = Load<TCompositeNodeBase::TTransientAttributes>(*context);
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
    return ENodeType::Map;
}

template <class TChild>
void TMapNodeImpl<TChild>::Save(NCellMaster::TSaveContext& context) const
{
    TCompositeNodeBase::Save(context);

    using NYT::Save;
    Save(context, ChildCountDelta_);
    Save(context, Children_);
}

template <class TChild>
void TMapNodeImpl<TChild>::Load(NCellMaster::TLoadContext& context)
{
    TCompositeNodeBase::Load(context);

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
        return TCompositeNodeBase::GetGCWeight();
    }
}

template <class TChild>
TDetailedMasterMemory TMapNodeImpl<TChild>::GetDetailedMasterMemoryUsage() const
{
    auto result = TCompositeNodeBase::GetDetailedMasterMemoryUsage();
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
    return ENodeType::Map;
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
        auto key = Load<std::string>(*context);
        auto childId = Load<TNodeId>(*context);
        context->RegisterChild(std::move(key), childId);
    }
}

// Explicit instantiations.
template class TCypressMapNodeTypeHandlerImpl<TCypressMapNode>;
template class TCypressMapNodeTypeHandlerImpl<TPortalExitNode>;
template class TCypressMapNodeTypeHandlerImpl<NMaintenanceTrackerServer::TClusterProxyNode>;

////////////////////////////////////////////////////////////////////////////////

namespace {

[[noreturn]] void ThrowSequoiaNodeCloningNotImplemented()
{
    THROW_ERROR_EXCEPTION("Sequoia node cloning is not implemented yet");
}

} // namespace

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
    TImpl* /*node*/,
    TSerializeNodeContext* /*context*/)
{
    ThrowSequoiaNodeCloningNotImplemented();
}

template <class TImpl>
void TSequoiaMapNodeTypeHandlerImpl<TImpl>::DoMaterializeNode(
    TImpl* /*trunkNode*/,
    TMaterializeNodeContext* /*context*/)
{
    ThrowSequoiaNodeCloningNotImplemented();
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
    TCompositeNodeBase::Save(context);

    SaveWith<TVectorSerializer<TRawNonversionedObjectPtrSerializer>>(context, IndexToChild_);
}

void TListNode::Load(NCellMaster::TLoadContext& context)
{
    TCompositeNodeBase::Load(context);

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
    YT_LOG_ALERT("Recieved EndCopy command for list node, despite BeginCopy being disabled for this type (ListNodeId: %v)",
        trunkNode->GetId());
    THROW_ERROR_EXCEPTION("List nodes are deprecated and do not support cross-cell copying");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
