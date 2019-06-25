#include "node_detail.h"
#include "helpers.h"
#include "node_proxy_detail.h"

#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>

#include <yt/server/master/security_server/account.h>
#include <yt/server/master/security_server/user.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const EObjectType TCypressScalarTypeTraits<TString>::ObjectType = EObjectType::StringNode;
const ENodeType   TCypressScalarTypeTraits<TString>::NodeType   = ENodeType::String;
const EObjectType TCypressScalarTypeTraits<i64>::ObjectType    = EObjectType::Int64Node;
const ENodeType   TCypressScalarTypeTraits<i64>::NodeType      = ENodeType::Int64;
const EObjectType TCypressScalarTypeTraits<ui64>::ObjectType   = EObjectType::Uint64Node;
const ENodeType   TCypressScalarTypeTraits<ui64>::NodeType     = ENodeType::Uint64;
const EObjectType TCypressScalarTypeTraits<double>::ObjectType = EObjectType::DoubleNode;
const ENodeType   TCypressScalarTypeTraits<double>::NodeType   = ENodeType::Double;
const EObjectType TCypressScalarTypeTraits<bool>::ObjectType   = EObjectType::BooleanNode;
const ENodeType   TCypressScalarTypeTraits<bool>::NodeType     = ENodeType::Boolean;


} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeTypeHandlerBase::TNontemplateCypressNodeTypeHandlerBase(
    NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

bool TNontemplateCypressNodeTypeHandlerBase::IsExternalizable() const
{
    return false;
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

void TNontemplateCypressNodeTypeHandlerBase::DestroyCore(TCypressNodeBase* node)
{
    // Reset parent links from immediate descendants.
    for (auto* descendant : node->ImmediateDescendants()) {
        descendant->ResetParent();
    }
    node->ImmediateDescendants().clear();
    node->SetParent(nullptr);

    // Clear ACD to unregister the node from linked objects.
    node->Acd().Clear();
}

void TNontemplateCypressNodeTypeHandlerBase::BranchCore(
    TCypressNodeBase* originatingNode,
    TCypressNodeBase* branchedNode,
    TTransaction* transaction,
    const TLockRequest& lockRequest)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();

    // Copy basic properties.
    branchedNode->SetParent(originatingNode->GetParent());
    branchedNode->SetCreationTime(originatingNode->GetCreationTime());
    branchedNode->SetModificationTime(originatingNode->GetModificationTime());
    branchedNode->SetAttributesRevision(originatingNode->GetAttributesRevision());
    branchedNode->SetContentRevision(originatingNode->GetContentRevision());
    branchedNode->SetLockMode(lockRequest.Mode);
    branchedNode->SetTrunkNode(originatingNode->GetTrunkNode());
    branchedNode->SetTransaction(transaction);
    branchedNode->SetOriginator(originatingNode);
    branchedNode->SetExternalCellTag(originatingNode->GetExternalCellTag());
    if (originatingNode->IsForeign()) {
        branchedNode->SetForeign();
    }
    branchedNode->SetOpaque(originatingNode->GetOpaque());

    // Copying node's account requires special handling.
    YCHECK(!branchedNode->GetAccount());
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* account = originatingNode->GetAccount();
    securityManager->SetAccount(branchedNode, nullptr, account, transaction);

    // Branch user attributes.
    objectManager->BranchAttributes(originatingNode, branchedNode);
}

void TNontemplateCypressNodeTypeHandlerBase::MergeCore(
    TCypressNodeBase* originatingNode,
    TCypressNodeBase* branchedNode)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();

    // Merge user attributes.
    objectManager->MergeAttributes(originatingNode, branchedNode);

    // Perform cleanup by resetting the parent link of the branched node.
    branchedNode->SetParent(nullptr);

    // Merge modification time.
    const auto* mutationContext = NHydra::GetCurrentMutationContext();
    originatingNode->SetModificationTime(mutationContext->GetTimestamp());
    originatingNode->SetAttributesRevision(mutationContext->GetVersion().ToRevision());
    originatingNode->SetContentRevision(mutationContext->GetVersion().ToRevision());
}

TCypressNodeBase* TNontemplateCypressNodeTypeHandlerBase::CloneCorePrologue(
    ICypressNodeFactory* factory,
    TNodeId hintId,
    TCellTag externalCellTag)
{
    auto type = GetObjectType();
    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto clonedId = hintId
        ? hintId
        : objectManager->GenerateId(type, NullObjectId);
    return factory->InstantiateNode(clonedId, externalCellTag);
}

void TNontemplateCypressNodeTypeHandlerBase::CloneCoreEpilogue(
    TCypressNodeBase* sourceNode,
    TCypressNodeBase* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode)
{
    // Copy attributes directly to suppress validation.
    auto keyToAttribute = GetNodeAttributes(
        Bootstrap_->GetCypressManager(),
        sourceNode->GetTrunkNode(),
        factory->GetTransaction());
    if (!keyToAttribute.empty()) {
        auto* clonedAttributes = clonedNode->GetMutableAttributes();
        for (const auto& pair : keyToAttribute) {
            YCHECK(clonedAttributes->Attributes().insert(pair).second);
        }
    }

    // Copy ACD, but only in move.
    if (mode == ENodeCloneMode::Move) {
        clonedNode->Acd().SetInherit(sourceNode->Acd().GetInherit());
        for (const auto& ace : sourceNode->Acd().Acl().Entries) {
            clonedNode->Acd().AddEntry(ace);
        }
    }

    // Copy builtin attributes.
    clonedNode->SetOpaque(sourceNode->GetOpaque());
    if (mode == ENodeCloneMode::Move) {
        clonedNode->SetCreationTime(sourceNode->GetCreationTime());
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TCompositeNodeBase::TAttributes::operator==(const TAttributes& rhs) const
{
    if (this == &rhs) {
        return true;
    }

#define XX(camelCaseName, snakeCaseName) \
    && camelCaseName == rhs.camelCaseName

    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);

#undef XX
}

bool TCompositeNodeBase::TAttributes::operator!=(const TAttributes& rhs) const
{
    return !(*this == rhs);
}

void TCompositeNodeBase::TAttributes::Persist(NCellMaster::TPersistenceContext& context)
{
#define XX(camelCaseName, snakeCaseName) \
    Persist(context, camelCaseName);

    using NYT::Persist;
    FOR_EACH_INHERITABLE_ATTRIBUTE(XX);

#undef XX
}

bool TCompositeNodeBase::TAttributes::AreFull() const
{
#define XX(camelCaseName, snakeCaseName) \
    && camelCaseName

    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);

#undef XX
}

bool TCompositeNodeBase::TAttributes::AreEmpty() const
{
#define XX(camelCaseName, snakeCaseName) \
    && !camelCaseName

    return true FOR_EACH_INHERITABLE_ATTRIBUTE(XX);

#undef XX
}

void TCompositeNodeBase::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    using NYT::Save;
    TUniquePtrSerializer<>::Save(context, Attributes_);
}

void TCompositeNodeBase::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    TUniquePtrSerializer<>::Load(context, Attributes_);
}

bool TCompositeNodeBase::HasInheritableAttributes() const
{
    if (Attributes_) {
        Y_ASSERT(!Attributes_->AreEmpty());
        return true;
    } else {
        return false;
    }
}

const TCompositeNodeBase::TAttributes* TCompositeNodeBase::Attributes() const
{
    return Attributes_.get();
}

void TCompositeNodeBase::SetAttributes(const TCompositeNodeBase::TAttributes* attributes)
{
    if (!attributes || attributes->AreEmpty()) {
        Attributes_.reset();
    } else if (Attributes_) {
        *Attributes_ = *attributes;
    } else {
        Attributes_ = std::make_unique<TAttributes>(*attributes);
    }
}

#define IMPLEMENT_ATTRIBUTE_ACCESSORS(camelCaseName, snakeCaseName) \
auto TCompositeNodeBase::Get##camelCaseName() const -> decltype(TCompositeNodeBase::TAttributes::camelCaseName) \
{ \
    if (Attributes_) { \
        return Attributes_->camelCaseName; \
    } else { \
        return {}; \
    } \
} \
void TCompositeNodeBase::Set##camelCaseName(decltype(TCompositeNodeBase::TAttributes::camelCaseName) value) \
{ \
    if (Attributes_) { \
        Attributes_->camelCaseName = value; \
        if (Attributes_->AreEmpty()) { \
            Attributes_.reset(); \
        } \
    } else if (value) { \
        Attributes_ = std::make_unique<TAttributes>(); \
        Attributes_->camelCaseName = value; \
    } \
}

FOR_EACH_INHERITABLE_ATTRIBUTE(IMPLEMENT_ATTRIBUTE_ACCESSORS)

#undef IMPLEMENT_ATTRIBUTE_ACCESSORS

////////////////////////////////////////////////////////////////////////////////

TMapNodeChildren::~TMapNodeChildren()
{
    YCHECK(KeyToChild.empty());
    YCHECK(ChildToKey.empty());
}

void TMapNodeChildren::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, KeyToChild);
}

void TMapNodeChildren::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, KeyToChild);

    // Reconstruct ChildToKey map.
    for (const auto& [key, childNode] : KeyToChild) {
        if (childNode) {
            YCHECK(ChildToKey.insert(std::make_pair(childNode, key)).second);
        }
    }
}

/*static*/ void TMapNodeChildren::Destroy(
    TMapNodeChildren* children,
    const TObjectManagerPtr& objectManager)
{
    YCHECK(children->GetRefCount() == 0);
    children->UnrefChildren(objectManager);

    children->KeyToChild.clear();
    children->ChildToKey.clear();

    delete children;
}

/*static*/ TMapNodeChildren* TMapNodeChildren::Copy(
    TMapNodeChildren* srcChildren,
    const TObjectManagerPtr& objectManager)
{
    YCHECK(srcChildren->GetRefCount() != 0);

    auto holder = std::make_unique<TMapNodeChildren>();
    holder->KeyToChild = srcChildren->KeyToChild;
    holder->ChildToKey = srcChildren->ChildToKey;

    holder->RefChildren(objectManager);

    return holder.release();
}

void TMapNodeChildren::RefChildren(const NObjectServer::TObjectManagerPtr& objectManager)
{
    // Make sure we handle children in a stable order.
    for (const auto& [key, childNode] : SortKeyToChild(KeyToChild)) {
        if (childNode) {
            objectManager->RefObject(childNode);
        }
    }
}

void TMapNodeChildren::UnrefChildren(const NObjectServer::TObjectManagerPtr& objectManager)
{
    // Make sure we handle children in a stable order.
    for (const auto& [key, childNode] : SortKeyToChild(KeyToChild)) {
        if (childNode) {
            objectManager->UnrefObject(childNode);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TMapNode::TMapNode(const TVersionedNodeId& id)
    : TCompositeNodeBase(id)
{ }

TMapNode::~TMapNode() = default;

const TMapNode::TKeyToChild& TMapNode::KeyToChild() const
{
    return Children_.Get().KeyToChild;
}

const TMapNode::TChildToKey& TMapNode::ChildToKey() const
{
    return Children_.Get().ChildToKey;
}

TMapNode::TKeyToChild& TMapNode::MutableKeyToChild(const TObjectManagerPtr& objectManager)
{
    return Children_.MutableGet(objectManager).KeyToChild;
}

TMapNode::TChildToKey& TMapNode::MutableChildToKey(const TObjectManagerPtr& objectManager)
{
    return Children_.MutableGet(objectManager).ChildToKey;
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

    // COMPAT(shakurov)
    if (context.GetVersion() < 835) {
        Children_.ResetToDefaultConstructed();
        // Passing a nullptr as the object manager is a dirty hack: in this
        // particular case, we're sure there's no CoW sharing, and the object
        // manager won't actually be used.
        auto& keyToChild = MutableKeyToChild(nullptr);
        auto& childToKey = MutableChildToKey(nullptr);
        TMapSerializer<
            TDefaultSerializer,
            TNonversionedObjectRefSerializer
        >::Load(context, keyToChild);

        // Reconstruct ChildToKey map.
        for (const auto& [key, childNode] : keyToChild) {
            if (childNode) {
                YCHECK(childToKey.insert(std::make_pair(childNode, key)).second);
            }
        }
    } else {
        Load(context, Children_);
    }
}

int TMapNode::GetGCWeight() const
{
    return TObjectBase::GetGCWeight() + KeyToChild().size();
}

////////////////////////////////////////////////////////////////////////////////

EObjectType TMapNodeTypeHandler::GetObjectType() const
{
    return EObjectType::MapNode;
}

ENodeType TMapNodeTypeHandler::GetNodeType() const
{
    return ENodeType::Map;
}

void TMapNodeTypeHandler::DoDestroy(TMapNode* node)
{
    TBase::DoDestroy(node);

    node->ChildCountDelta_ = 0;
    node->Children_.Reset(Bootstrap_->GetObjectManager());
}

void TMapNodeTypeHandler::DoBranch(
    const TMapNode* originatingNode,
    TMapNode* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    YCHECK(!branchedNode->Children_);

    if (lockRequest.Mode == ELockMode::Snapshot) {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        if (originatingNode->IsTrunk()) {
            branchedNode->ChildCountDelta() = originatingNode->ChildCountDelta();
            branchedNode->Children_.Assign(originatingNode->Children_, objectManager);
        } else {
            const auto& cypressManager = Bootstrap_->GetCypressManager();

            THashMap<TString, TCypressNodeBase*> keyToChildStorage;
            const auto& originatingNodeChildren = GetMapNodeChildMap(
                cypressManager,
                originatingNode->GetTrunkNode(),
                originatingNode->GetTransaction(),
                &keyToChildStorage);

            branchedNode->ChildCountDelta() = originatingNodeChildren.size();
            branchedNode->MutableKeyToChild(objectManager) = originatingNodeChildren;
            for (const auto& [key, childNode] : SortKeyToChild(branchedNode->KeyToChild())) {
                if (childNode) {
                    objectManager->RefObject(childNode);
                }
            }
        }
    }

    // Non-snapshot branches only hold changes, i.e. deltas. Which are empty at first.
}

void TMapNodeTypeHandler::DoMerge(
    TMapNode* originatingNode,
    TMapNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    const auto& objectManager = Bootstrap_->GetObjectManager();

    bool isOriginatingNodeBranched = originatingNode->GetTransaction() != nullptr;

    auto& keyToChild = originatingNode->MutableKeyToChild(objectManager);
    auto& childToKey = originatingNode->MutableChildToKey(objectManager);

    for (const auto& pair : SortKeyToChild(branchedNode->KeyToChild())) {
        const auto& key = pair.first;
        auto* childTrunkNode = pair.second;

        auto it = keyToChild.find(key);
        if (childTrunkNode) {

            objectManager->RefObject(childTrunkNode);

            if (it == keyToChild.end()) {
                // Originating: missing
                YCHECK(childToKey.insert(std::make_pair(childTrunkNode, key)).second);
                YCHECK(keyToChild.insert(std::make_pair(key, childTrunkNode)).second);
            } else if (it->second) {
                // Originating: present
                objectManager->UnrefObject(it->second);
                YCHECK(childToKey.erase(it->second) == 1);
                YCHECK(childToKey.insert(std::make_pair(childTrunkNode, key)).second);
                it->second = childTrunkNode;
            } else {
                // Originating: tombstone
                it->second = childTrunkNode;
                YCHECK(childToKey.insert(std::make_pair(childTrunkNode, key)).second);
            }
        } else {
            // Branched: tombstone
            if (it == keyToChild.end()) {
                // Originating: missing
                if (isOriginatingNodeBranched) {
                    YCHECK(keyToChild.insert(std::make_pair(key, nullptr)).second);
                }
            } else if (it->second) {
                // Originating: present
                objectManager->UnrefObject(it->second);
                YCHECK(childToKey.erase(it->second) == 1);
                if (isOriginatingNodeBranched) {
                    it->second = nullptr;
                } else {
                    keyToChild.erase(it);
                }
            } else {
                // Originating: tombstone
            }
        }
    }

    originatingNode->ChildCountDelta() += branchedNode->ChildCountDelta();

    branchedNode->Children_.Reset(objectManager);
}

ICypressNodeProxyPtr TMapNodeTypeHandler::DoGetProxy(
    TMapNode* trunkNode,
    TTransaction* transaction)
{
    return New<TMapNodeProxy>(
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

void TMapNodeTypeHandler::DoClone(
    TMapNode* sourceNode,
    TMapNode* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    auto* transaction = factory->GetTransaction();

    const auto& cypressManager = Bootstrap_->GetCypressManager();

    THashMap<TString, TCypressNodeBase*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        sourceNode->GetTrunkNode(),
        transaction,
        &keyToChildMapStorage);
    auto keyToChildList = SortKeyToChild(keyToChildMap);

    auto* clonedTrunkNode = clonedNode->GetTrunkNode();

    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto& clonedNodeKeyToChild = clonedNode->MutableKeyToChild(objectManager);
    auto& clonedNodeChildToKey = clonedNode->MutableChildToKey(objectManager);

    for (const auto& pair : keyToChildList) {
        const auto& key = pair.first;
        auto* childTrunkNode = pair.second;

        auto* childNode = cypressManager->GetVersionedNode(childTrunkNode, transaction);

        auto* clonedChildNode = factory->CloneNode(childNode, mode);
        auto* clonedTrunkChildNode = clonedChildNode->GetTrunkNode();

        YCHECK(clonedNodeKeyToChild.insert(std::make_pair(key, clonedTrunkChildNode)).second);
        YCHECK(clonedNodeChildToKey.insert(std::make_pair(clonedTrunkChildNode, key)).second);

        AttachChild(objectManager, clonedTrunkNode, clonedChildNode);

        ++clonedNode->ChildCountDelta();
    }
}

bool TMapNodeTypeHandler::HasBranchedChangesImpl(TMapNode* originatingNode, TMapNode* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    if (branchedNode->GetLockMode() == ELockMode::Snapshot) {
        return false;
    }

    return !branchedNode->KeyToChild().empty();
}

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
        TNonversionedObjectRefSerializer
    >::Save(context, IndexToChild_);
}

void TListNode::Load(NCellMaster::TLoadContext& context)
{
    TCompositeNodeBase::Load(context);

    using NYT::Load;
    TVectorSerializer<
        TNonversionedObjectRefSerializer
    >::Load(context, IndexToChild_);

    // Reconstruct ChildToIndex.
    for (int index = 0; index < IndexToChild_.size(); ++index) {
        YCHECK(ChildToIndex_.insert(std::make_pair(IndexToChild_[index], index)).second);
    }
}

int TListNode::GetGCWeight() const
{
    return TObjectBase::GetGCWeight() + IndexToChild_.size();
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
    TBase::DoDestroy(node);

    // Drop references to the children.
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* child : node->IndexToChild()) {
        objectManager->UnrefObject(child);
    }
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
    TListNode* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    auto* clonedTrunkNode = clonedNode->GetTrunkNode();

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& indexToChild = sourceNode->IndexToChild();
    for (int index = 0; index < indexToChild.size(); ++index) {
        auto* childNode = indexToChild[index];
        auto* clonedChildNode = factory->CloneNode(childNode, mode);
        auto* clonedChildTrunkNode = clonedChildNode->GetTrunkNode();

        clonedNode->IndexToChild().push_back(clonedChildTrunkNode);
        YCHECK(clonedNode->ChildToIndex().insert(std::make_pair(clonedChildTrunkNode, index)).second);

        AttachChild(objectManager, clonedTrunkNode, clonedChildNode);
    }
}

bool TListNodeTypeHandler::HasBranchedChangesImpl(TListNode* originatingNode, TListNode* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    return branchedNode->IndexToChild() != originatingNode->IndexToChild();
}

////////////////////////////////////////////////////////////////////////////////

ENodeType TLinkNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TLinkNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    using NYT::Save;
    Save(context, TargetPath_);
}

void TLinkNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    Load(context, TargetPath_);
}

////////////////////////////////////////////////////////////////////////////////

TLinkNodeTypeHandler::TLinkNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

EObjectType TLinkNodeTypeHandler::GetObjectType() const
{
    return EObjectType::Link;
}

ENodeType TLinkNodeTypeHandler::GetNodeType() const
{
    return ENodeType::Entity;
}

ICypressNodeProxyPtr TLinkNodeTypeHandler::DoGetProxy(
    TLinkNode* trunkNode,
    TTransaction* transaction)
{
    return New<TLinkNodeProxy>(
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

std::unique_ptr<TLinkNode> TLinkNodeTypeHandler::DoCreate(
    const TVersionedNodeId& id,
    TCellTag cellTag,
    TTransaction* transaction,
    IAttributeDictionary* inheritedAttributes,
    IAttributeDictionary* explicitAttributes,
    TAccount* account)
{
    // Make sure that target_path is valid upon creation.
    auto targetPath = explicitAttributes->GetAndRemove<TString>("target_path");
    const auto& objectManager = Bootstrap_->GetObjectManager();
    objectManager->ResolvePathToObject(targetPath, transaction);

    auto implHolder = TBase::DoCreate(
        id,
        cellTag,
        transaction,
        inheritedAttributes,
        explicitAttributes,
        account);

    implHolder->SetTargetPath(targetPath);

    YT_LOG_DEBUG("Link created (LinkId: %v, TargetPath: %v)",
        id,
        targetPath);

    return implHolder;
}

void TLinkNodeTypeHandler::DoBranch(
    const TLinkNode* originatingNode,
    TLinkNode* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    branchedNode->SetTargetPath(originatingNode->GetTargetPath());
}

void TLinkNodeTypeHandler::DoMerge(
    TLinkNode* originatingNode,
    TLinkNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    originatingNode->SetTargetPath(branchedNode->GetTargetPath());
}

void TLinkNodeTypeHandler::DoClone(
    TLinkNode* sourceNode,
    TLinkNode* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    clonedNode->SetTargetPath(sourceNode->GetTargetPath());
}

bool TLinkNodeTypeHandler::HasBranchedChangesImpl(
    TLinkNode* originatingNode,
    TLinkNode* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    return branchedNode->GetTargetPath() != originatingNode->GetTargetPath();
}

////////////////////////////////////////////////////////////////////////////////

TDocumentNode::TDocumentNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , Value_(GetEphemeralNodeFactory()->CreateEntity())
{ }

ENodeType TDocumentNode::GetNodeType() const
{
    return ENodeType::Entity;
}

void TDocumentNode::Save(NCellMaster::TSaveContext& context) const
{
    TCypressNodeBase::Save(context);

    using NYT::Save;
    auto serializedValue = ConvertToYsonStringStable(Value_);
    Save(context, serializedValue.GetData());
}

void TDocumentNode::Load(NCellMaster::TLoadContext& context)
{
    TCypressNodeBase::Load(context);

    using NYT::Load;
    auto serializedValue = Load<TString>(context);
    Value_ = ConvertToNode(TYsonString(serializedValue));
}

////////////////////////////////////////////////////////////////////////////////

TDocumentNodeTypeHandler::TDocumentNodeTypeHandler(NCellMaster::TBootstrap* bootstrap)
    : TBase(bootstrap)
{ }

EObjectType TDocumentNodeTypeHandler::GetObjectType() const
{
    return EObjectType::Document;
}

ENodeType TDocumentNodeTypeHandler::GetNodeType() const
{
    return ENodeType::Entity;
}

ICypressNodeProxyPtr TDocumentNodeTypeHandler::DoGetProxy(
    TDocumentNode* trunkNode,
    TTransaction* transaction)
{
    return New<TDocumentNodeProxy>(
        Bootstrap_,
        &Metadata_,
        transaction,
        trunkNode);
}

void TDocumentNodeTypeHandler::DoBranch(
    const TDocumentNode* originatingNode,
    TDocumentNode* branchedNode,
    const TLockRequest& lockRequest)
{
    TBase::DoBranch(originatingNode, branchedNode, lockRequest);

    branchedNode->SetValue(CloneNode(originatingNode->GetValue()));
}

void TDocumentNodeTypeHandler::DoMerge(
    TDocumentNode* originatingNode,
    TDocumentNode* branchedNode)
{
    TBase::DoMerge(originatingNode, branchedNode);

    originatingNode->SetValue(branchedNode->GetValue());
}

void TDocumentNodeTypeHandler::DoClone(
    TDocumentNode* sourceNode,
    TDocumentNode* clonedNode,
    ICypressNodeFactory* factory,
    ENodeCloneMode mode,
    TAccount* account)
{
    TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

    clonedNode->SetValue(CloneNode(sourceNode->GetValue()));
}

bool TDocumentNodeTypeHandler::HasBranchedChangesImpl(
    TDocumentNode* originatingNode,
    TDocumentNode* branchedNode)
{
    if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
        return true;
    }

    return !AreNodesEqual(branchedNode->GetValue(), originatingNode->GetValue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

