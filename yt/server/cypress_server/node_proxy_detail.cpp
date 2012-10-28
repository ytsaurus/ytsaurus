#include "stdafx.h"
#include "node_proxy_detail.h"
#include "helpers.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NRpc;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TVersionedUserAttributeDictionary::TVersionedUserAttributeDictionary(
    const TObjectId& id,
    TTransaction* transaction,
    TBootstrap* bootstrap)
    : Id(id)
    , Transaction(transaction)
    , Bootstrap(bootstrap)
{ }

std::vector<Stroka> TVersionedUserAttributeDictionary::List() const 
{
    auto keyToAttribute = GetNodeAttributes(Bootstrap, Id, Transaction);
    std::vector<Stroka> keys;
    FOREACH (const auto& pair, keyToAttribute) {
        keys.push_back(pair.first);
    }
    return keys;
}

TNullable<TYsonString> TVersionedUserAttributeDictionary::FindYson(const Stroka& name) const 
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);

    FOREACH (const auto* transaction, transactions) {
        TVersionedObjectId versionedId(Id, GetObjectId(transaction));
        const auto* userAttributes = objectManager->FindAttributes(versionedId);
        if (userAttributes) {
            auto it = userAttributes->Attributes().find(name);
            if (it != userAttributes->Attributes().end()) {
                return it->second;
            }
        }
    }

    return Null;
}

void TVersionedUserAttributeDictionary::SetYson(const Stroka& key, const TYsonString& value)
{
    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();

    auto* node = cypressManager->LockVersionedNode(
        Id,
        Transaction,
        TLockRequest::SharedAttribute(key));
    auto versionedId = node->GetId();

    auto* userAttributes = objectManager->FindAttributes(versionedId);
    if (!userAttributes) {
        userAttributes = objectManager->CreateAttributes(versionedId);
    }

    userAttributes->Attributes()[key] = value;

    cypressManager->SetModified(Id, Transaction);
}

bool TVersionedUserAttributeDictionary::Remove(const Stroka& key)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto objectManager = Bootstrap->GetObjectManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    std::reverse(transactions.begin(), transactions.end());

    const NTransactionServer::TTransaction* containingTransaction = NULL;
    bool contains = false;
    FOREACH (const auto* transaction, transactions) {
        TVersionedObjectId versionedId(Id, GetObjectId(transaction));
        const auto* userAttributes = objectManager->FindAttributes(versionedId);
        if (userAttributes) {
            auto it = userAttributes->Attributes().find(key);
            if (it != userAttributes->Attributes().end()) {
                contains = it->second;
                if (contains) {
                    containingTransaction = transaction;
                }
                break;
            }
        }
    }

    if (!contains) {
        return false;
    }

    auto* node = cypressManager->LockVersionedNode(
        Id,
        Transaction,
        TLockRequest::SharedAttribute(key));
    auto versionedId = node->GetId();

    if (containingTransaction == Transaction) {
        auto* userAttributes = objectManager->GetAttributes(versionedId);
        YCHECK(userAttributes->Attributes().erase(key) == 1);
    } else {
        YCHECK(!containingTransaction);
        auto* userAttributes = objectManager->FindAttributes(versionedId);
        if (!userAttributes) {
            userAttributes = objectManager->CreateAttributes(versionedId);
        }
        userAttributes->Attributes()[key] = Null;
    }

    cypressManager->SetModified(Id, Transaction);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TCypressNodeProxyNontemplateBase::TCypressNodeProxyNontemplateBase(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    ICypressNode* trunkNode)
    : TObjectProxyBase(bootstrap, trunkNode->GetId().ObjectId)
    , TypeHandler(typeHandler)
    , Bootstrap(bootstrap)
    , Transaction(transaction)
    , TrunkNode(trunkNode)
{
    YASSERT(typeHandler);
    YASSERT(bootstrap);
    YASSERT(trunkNode);

    Logger = NLog::TLogger("Cypress");
}

INodeFactoryPtr TCypressNodeProxyNontemplateBase::CreateFactory() const
{
    return New<TNodeFactory>(Bootstrap, Transaction);
}

IYPathResolverPtr TCypressNodeProxyNontemplateBase::GetResolver() const
{
    if (!Resolver) {
        auto cypressManager = Bootstrap->GetCypressManager();
        Resolver = cypressManager->CreateResolver(Transaction);
    }
    return Resolver;
}

NTransactionServer::TTransaction* TCypressNodeProxyNontemplateBase::GetTransaction() const 
{
    return Transaction;
}

ICypressNode* TCypressNodeProxyNontemplateBase::GetTrunkNode() const 
{
    return TrunkNode;
}

ENodeType TCypressNodeProxyNontemplateBase::GetType() const 
{
    return TypeHandler->GetNodeType();
}

ICompositeNodePtr TCypressNodeProxyNontemplateBase::GetParent() const 
{
    auto nodeId = GetThisImpl()->GetParentId();
    return nodeId == NObjectClient::NullObjectId ? NULL : GetProxy(nodeId)->AsComposite();
}

void TCypressNodeProxyNontemplateBase::SetParent(ICompositeNodePtr parent)
{
    auto* impl = LockThisImpl();
    impl->SetParentId(parent ? GetNodeId(INodePtr(parent)) : NObjectClient::NullObjectId);
}

bool TCypressNodeProxyNontemplateBase::IsWriteRequest(NRpc::IServiceContextPtr context) const 
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Lock);
    // NB: Create is not considered a write verb since it always fails here.
    return TNodeBase::IsWriteRequest(context);
}

IAttributeDictionary& TCypressNodeProxyNontemplateBase::Attributes()
{
    return TObjectProxyBase::Attributes();
}

const IAttributeDictionary& TCypressNodeProxyNontemplateBase::Attributes() const 
{
    return TObjectProxyBase::Attributes();
}

void TCypressNodeProxyNontemplateBase::GetAttributes(
    IYsonConsumer* consumer,
    const TAttributeFilter& filter) const 
{
    if (filter.Mode == EAttributeFilterMode::None)
        return;

    const auto& userAttributes = Attributes();

    auto userKeys = userAttributes.List();

    std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
    ListSystemAttributes(&systemAttributes);

    yhash_set<Stroka> matchingKeys(filter.Keys.begin(), filter.Keys.end());

    bool seenMatching = false;

    FOREACH (const auto& key, userKeys) {
        if (filter.Mode == EAttributeFilterMode::All || matchingKeys.find(key) != matchingKeys.end()) {
            if (!seenMatching) {
                consumer->OnBeginAttributes();
                seenMatching = true;
            }
            consumer->OnKeyedItem(key);
            consumer->OnRaw(userAttributes.GetYson(key).Data(), EYsonType::Node);
        }
    }

    FOREACH (const auto& attribute, systemAttributes) {
        if (attribute.IsPresent &&
            (filter.Mode == EAttributeFilterMode::All || matchingKeys.find(attribute.Key) != matchingKeys.end()))
        {
            if (!seenMatching) {
                consumer->OnBeginAttributes();
                seenMatching = true;
            }
            Stroka key(attribute.Key);
            consumer->OnKeyedItem(key);
            if (attribute.IsOpaque) {
                consumer->OnEntity();
            } else {
                YCHECK(GetSystemAttribute(key, consumer));
            }
        }
    }

    if (seenMatching) {
        consumer->OnEndAttributes();
    }
}

TVersionedObjectId TCypressNodeProxyNontemplateBase::GetVersionedId() const 
{
    return TVersionedObjectId(Id, GetObjectId(Transaction));
}

void TCypressNodeProxyNontemplateBase::ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const 
{
    attributes->push_back("parent_id");
    attributes->push_back("locks");
    attributes->push_back("lock_mode");
    attributes->push_back(TAttributeInfo("path", true, true));
    attributes->push_back("creation_time");
    attributes->push_back("modification_time");
    attributes->push_back(TAttributeInfo("resource_usage", true, true));
    TObjectProxyBase::ListSystemAttributes(attributes);
}

bool TCypressNodeProxyNontemplateBase::GetSystemAttribute(
    const Stroka& key,
    IYsonConsumer* consumer) const 
{
    const auto* node = GetThisImpl();

    // NB: Locks are stored in trunk nodes (TransactionId == Null).
    const auto* trunkNode = Bootstrap->GetCypressManager()->GetNode(Id);

    if (key == "parent_id") {
        BuildYsonFluently(consumer)
            .Scalar(node->GetParentId().ToString());
        return true;
    }

    if (key == "locks") {
        BuildYsonFluently(consumer)
            .DoListFor(trunkNode->Locks(), [=] (TFluentList fluent, const ICypressNode::TLockMap::value_type& pair) {
                fluent.Item()
                    .BeginMap()
                    .Item("mode").Scalar(pair.second.Mode)
                    .Item("transaction_id").Scalar(pair.first->GetId())
                    .DoIf(!pair.second.ChildKeys.empty(), [=] (TFluentMap fluent) {
                        fluent
                            .Item("child_keys").List(pair.second.ChildKeys);
                    })
                    .DoIf(!pair.second.AttributeKeys.empty(), [=] (TFluentMap fluent) {
                        fluent
                            .Item("attribute_keys").List(pair.second.AttributeKeys);
                    })
                    .EndMap();
        });
        return true;
    }

    if (key == "lock_mode") {
        BuildYsonFluently(consumer)
            .Scalar(FormatEnum(node->GetLockMode()));
        return true;
    }

    if (key == "path") {
        BuildYsonFluently(consumer)
            .Scalar(GetPath());
        return true;
    }

    if (key == "creation_time") {
        BuildYsonFluently(consumer)
            .Scalar(node->GetCreationTime().ToString());
        return true;
    }

    if (key == "modification_time") {
        BuildYsonFluently(consumer)
            .Scalar(node->GetModificationTime().ToString());
        return true;
    }

    if (key == "resource_usage") {
        BuildYsonFluently(consumer)
            .Scalar(GetResourceUsage());
        return true;
    }

    return TObjectProxyBase::GetSystemAttribute(key, consumer);
}

void TCypressNodeProxyNontemplateBase::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Lock);
    DISPATCH_YPATH_SERVICE_METHOD(Create);
    TNodeBase::DoInvoke(context);
}

DEFINE_RPC_SERVICE_METHOD(TCypressNodeProxyNontemplateBase, Lock)
{
    auto mode = ELockMode(request->mode());

    context->SetRequestInfo("Mode: %s", ~mode.ToString());
    if (mode != ELockMode::Snapshot &&
        mode != ELockMode::Shared &&
        mode != ELockMode::Exclusive)
    {
        THROW_ERROR_EXCEPTION("Invalid lock mode: %s",
            ~mode.ToString());
    }

    if (!Transaction) {
        THROW_ERROR_EXCEPTION("Cannot take a lock outside of a transaction");
    }

    auto cypressManager = Bootstrap->GetCypressManager();
    cypressManager->LockVersionedNode(Id, Transaction, mode);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TCypressNodeProxyNontemplateBase, Create)
{
    UNUSED(request);
    UNUSED(response);

    NYPath::TTokenizer tokenizer(context->GetPath());
    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        THROW_ERROR_EXCEPTION("Node already exists: %s", ~this->GetPath());
    }

    ThrowVerbNotSuppored(this, context->GetVerb());
}

const ICypressNode* TCypressNodeProxyNontemplateBase::GetImpl(const TNodeId& nodeId) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->GetVersionedNode(nodeId, Transaction);
}

ICypressNode* TCypressNodeProxyNontemplateBase::LockImpl(
    const TNodeId& nodeId,
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->LockVersionedNode(nodeId, Transaction, request, recursive);
}

const ICypressNode* TCypressNodeProxyNontemplateBase::GetThisImpl() const
{
    return GetImpl(Id);
}

ICypressNode* TCypressNodeProxyNontemplateBase::LockThisImpl(
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/)
{
    return LockImpl(Id, request, recursive);
}

ICypressNodeProxyPtr TCypressNodeProxyNontemplateBase::GetProxy(const TNodeId& nodeId) const
{
    YASSERT(nodeId != NObjectClient::NullObjectId);
    return Bootstrap->GetCypressManager()->GetVersionedNodeProxy(nodeId, Transaction);
}

ICypressNodeProxyPtr TCypressNodeProxyNontemplateBase::ToProxy(INodePtr node)
{
    return dynamic_cast<ICypressNodeProxy*>(~node);
}

TNodeId TCypressNodeProxyNontemplateBase::GetNodeId(INodePtr node)
{
    return dynamic_cast<ICypressNodeProxy&>(*node).GetId();
}

TNodeId TCypressNodeProxyNontemplateBase::GetNodeId(IConstNodePtr node)
{
    return dynamic_cast<const ICypressNodeProxy&>(*node).GetId();
}

void TCypressNodeProxyNontemplateBase::AttachChild(ICypressNode* child)
{
    child->SetParentId(Id);
    Bootstrap->GetObjectManager()->RefObject(child);
}

void TCypressNodeProxyNontemplateBase::DetachChild(ICypressNode* child, bool unref)
{
    child->SetParentId(NObjectClient::NullObjectId);
    if (unref) {
        Bootstrap->GetObjectManager()->UnrefObject(child);
    }
}

TAutoPtr<IAttributeDictionary> TCypressNodeProxyNontemplateBase::DoCreateUserAttributes()
{
    return new TVersionedUserAttributeDictionary(
        Id,
        Transaction,
        Bootstrap);
}

void TCypressNodeProxyNontemplateBase::SetModified()
{
    Bootstrap->GetCypressManager()->SetModified(Id, Transaction);
}

TClusterResources TCypressNodeProxyNontemplateBase::GetResourceUsage() const 
{
    return ZeroClusterResources();
}

////////////////////////////////////////////////////////////////////////////////

TNodeFactory::TNodeFactory(
    TBootstrap* bootstrap,
    TTransaction* transaction)
    : Bootstrap(bootstrap)
    , Transaction(transaction)
{
    YCHECK(bootstrap);
}

TNodeFactory::~TNodeFactory()
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& nodeId, CreatedNodeIds) {
        objectManager->UnrefObject(nodeId);
    }
}

ICypressNodeProxyPtr TNodeFactory::DoCreate(EObjectType type)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto objectManager = Bootstrap->GetObjectManager();
   
    auto handler = cypressManager->GetHandler(type);
  
    auto node = handler->Create(Transaction, NULL, NULL);
    auto node_ = ~node;
    cypressManager->RegisterNode(Transaction, node);
    
    auto nodeId = node_->GetId().ObjectId;
    objectManager->RefObject(node_);
    CreatedNodeIds.push_back(nodeId);

    return cypressManager->GetVersionedNodeProxy(nodeId, Transaction);
}

IStringNodePtr TNodeFactory::CreateString()
{
    return DoCreate(EObjectType::StringNode)->AsString();
}

IIntegerNodePtr TNodeFactory::CreateInteger()
{
    return DoCreate(EObjectType::IntegerNode)->AsInteger();
}

IDoubleNodePtr TNodeFactory::CreateDouble()
{
    return DoCreate(EObjectType::DoubleNode)->AsDouble();
}

IMapNodePtr TNodeFactory::CreateMap()
{
    return DoCreate(EObjectType::MapNode)->AsMap();
}

IListNodePtr TNodeFactory::CreateList()
{
    return DoCreate(EObjectType::ListNode)->AsList();
}

IEntityNodePtr TNodeFactory::CreateEntity()
{
    THROW_ERROR_EXCEPTION("Entity nodes cannot be created inside Cypress");
}

////////////////////////////////////////////////////////////////////////////////

TMapNodeProxy::TMapNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    ICypressNode* trunkNode)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

void TMapNodeProxy::Clear()
{
    // Take shared lock for the node itself.
    auto* impl = LockThisTypedImpl(ELockMode::Shared);

    // Construct children list.
    auto keyToChild = GetMapNodeChildren(Bootstrap, Id, Transaction);

    // Take exclusive locks for children.
    typedef std::pair<Stroka, ICypressNode*> TChild;
    std::vector<TChild> children;
    FOREACH (const auto& pair, keyToChild) {
        LockThisImpl(TLockRequest::SharedChild(pair.first));
        auto* child = LockImpl(pair.second);
        children.push_back(std::make_pair(pair.first, child));
    }

    // Sort the children by the key to ensure consistent unref order.
    std::sort(
        children.begin(),
        children.end(),
        [] (const TChild& lhs, const TChild& rhs) {
            return lhs.first < rhs.first;
        });

    // Detach children.
    // Insert tombstones.
    FOREACH (const auto& pair, children) {
        const auto& key = pair.first;
        auto* child = pair.second;
        const auto& childId = child->GetId().ObjectId;
        if (impl->KeyToChild().find(key) != impl->KeyToChild().end()) {
            YCHECK(impl->KeyToChild().erase(key) == 1);
            YCHECK(impl->ChildToKey().erase(childId) == 1);
            DetachChild(child, true);
        } else {
            YCHECK(impl->KeyToChild().insert(std::make_pair(key, NullObjectId)).second);
            DetachChild(child, false);
        }
        --impl->ChildCountDelta();
    }

    SetModified();
}

int TMapNodeProxy::GetChildCount() const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    // NB: No need to reverse transactions.

    int result = 0;
    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedNodeId versionedId(Id, GetObjectId(currentTransaction));
        const auto* node = cypressManager->FindNode(versionedId);
        if (node) {
            const auto* mapNode = static_cast<const TMapNode*>(node);
            result += mapNode->ChildCountDelta();
        }
    }
    return result;
}

std::vector< TPair<Stroka, INodePtr> > TMapNodeProxy::GetChildren() const
{
    auto keyToChild = GetMapNodeChildren(Bootstrap, Id, Transaction);

    std::vector< TPair<Stroka, INodePtr> > result;
    result.reserve(keyToChild.size());
    FOREACH (const auto& pair, keyToChild) {
        result.push_back(std::make_pair(pair.first, GetProxy(pair.second)));
    }
    return result;
}

std::vector<Stroka> TMapNodeProxy::GetKeys() const
{
    auto keyToChild = GetMapNodeChildren(Bootstrap, Id, Transaction);

    std::vector<Stroka> result;
    FOREACH (const auto& pair, keyToChild) {
        result.push_back(pair.first);
    }
    return result;
}

INodePtr TMapNodeProxy::FindChild(const Stroka& key) const
{
    auto versionedChildId = FindMapNodeChild(Bootstrap, Id, Transaction, key);
    return versionedChildId.ObjectId == NullObjectId ? NULL : GetProxy(versionedChildId.ObjectId);
}

bool TMapNodeProxy::AddChild(INodePtr child, const Stroka& key)
{
    YASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));

    auto childId = GetNodeId(child);
    auto* childImpl = LockImpl(childId);

    impl->KeyToChild()[key] = childId;
    YCHECK(impl->ChildToKey().insert(MakePair(childId, key)).second);
    ++impl->ChildCountDelta();

    AttachChild(childImpl);

    SetModified();
    return true;
}

bool TMapNodeProxy::RemoveChild(const Stroka& key)
{
    auto versionedChildId = FindMapNodeChild(Bootstrap, Id, Transaction, key);
    if (versionedChildId.ObjectId == NullObjectId) {
        return false;
    }

    const auto& childId = versionedChildId.ObjectId;
    auto* childImpl = LockImpl(childId, ELockMode::Exclusive, true);
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));

    if (versionedChildId.TransactionId == GetObjectId(Transaction)) {
        YCHECK(impl->KeyToChild().erase(key) == 1);
        YCHECK(impl->ChildToKey().erase(childId) == 1);
        DetachChild(childImpl, true);
    } else {
        YCHECK(impl->KeyToChild().insert(MakePair(key, NullObjectId)).second);
        DetachChild(childImpl, false);
    }

    --impl->ChildCountDelta();

    SetModified();
    return true;
}

void TMapNodeProxy::RemoveChild(INodePtr child)
{
    auto key = GetChildKey(child);
    auto childId = GetNodeId(child);

    auto* childImpl = LockImpl(childId, ELockMode::Exclusive, true);
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));

    auto it = impl->ChildToKey().find(childId);
    if (it != impl->ChildToKey().end()) {
        YCHECK(impl->KeyToChild().erase(key) == 1);
        YCHECK(impl->ChildToKey().erase(childId) == 1);
        DetachChild(childImpl, true);
    } else {
        YCHECK(impl->KeyToChild().insert(MakePair(key, NullObjectId)).second);
        DetachChild(childImpl, false);
    }

    --impl->ChildCountDelta();

    SetModified();
}

void TMapNodeProxy::ReplaceChild(INodePtr oldChild, INodePtr newChild)
{
    if (oldChild == newChild)
        return;

    auto key = GetChildKey(oldChild);

    auto oldChildId = GetNodeId(oldChild);
    auto* oldChildImpl = LockImpl(oldChildId, ELockMode::Exclusive, true);

    auto newChildId = GetNodeId(newChild);
    auto* newChildImpl = LockImpl(newChildId);

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    impl->KeyToChild()[key] = newChildId;
    bool ownsOldChild = impl->KeyToChild().find(key) != impl->KeyToChild().end();
    DetachChild(oldChildImpl, ownsOldChild);
    YCHECK(impl->ChildToKey().insert(MakePair(newChildId, key)).second);    
    AttachChild(newChildImpl);

    SetModified();
}

Stroka TMapNodeProxy::GetChildKey(IConstNodePtr child)
{
    auto childId = GetNodeId(child);

    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    // NB: Use the latest key, don't reverse transactions.
    
    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedNodeId versionedId(Id, GetObjectId(currentTransaction));
        const auto* node = cypressManager->FindNode(versionedId);
        if (node) {
            const auto* mapNode = static_cast<const TMapNode*>(node);
            auto it = mapNode->ChildToKey().find(childId);
            if (it != mapNode->ChildToKey().end()) {
                return it->second;
            }
        }
    }

    YUNREACHABLE();
}

void TMapNodeProxy::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    TBase::DoInvoke(context);
}

void TMapNodeProxy::SetRecursive(const TYPath& path, INodePtr value)
{
    TMapNodeMixin::SetRecursive(path, value);
}

IYPathService::TResolveResult TMapNodeProxy::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    return TMapNodeMixin::ResolveRecursive(path, context);
}

////////////////////////////////////////////////////////////////////////////////

TListNodeProxy::TListNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    ICypressNode* trunkNode)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

void TListNodeProxy::Clear()
{
    auto* impl = LockThisTypedImpl();

    // Validate locks and obtain impls first.
    std::vector<ICypressNode*> children;
    FOREACH (const auto& nodeId, impl->IndexToChild()) {
        children.push_back(LockImpl(nodeId));
    }

    FOREACH (auto* child, children) {
        DetachChild(child, true);
    }

    impl->IndexToChild().clear();
    impl->ChildToIndex().clear();

    SetModified();
}

int TListNodeProxy::GetChildCount() const
{
    const auto* impl = GetThisTypedImpl();
    return impl->IndexToChild().size();
}

std::vector<INodePtr> TListNodeProxy::GetChildren() const
{
    std::vector<INodePtr> result;
    const auto* impl = GetThisTypedImpl();
    const auto& indexToChild = impl->IndexToChild();
    result.reserve(indexToChild.size());
    FOREACH (const auto& nodeId, indexToChild) {
        result.push_back(GetProxy(nodeId));
    }
    return result;
}

INodePtr TListNodeProxy::FindChild(int index) const
{
    const auto* impl = GetThisTypedImpl();
    const auto& indexToChild = impl->IndexToChild();
    return index >= 0 && index < indexToChild.size() ? GetProxy(indexToChild[index]) : NULL;
}

void TListNodeProxy::AddChild(INodePtr child, int beforeIndex /*= -1*/)
{
    auto* impl = LockThisTypedImpl();
    auto& list = impl->IndexToChild();

    auto childId = GetNodeId(child);
    auto* childImpl = LockImpl(childId);

    if (beforeIndex < 0) {
        YCHECK(impl->ChildToIndex().insert(MakePair(childId, list.size())).second);
        list.push_back(childId);
    } else {
        // Update indices.
        for (auto it = list.begin() + beforeIndex; it != list.end(); ++it) {
            ++impl->ChildToIndex()[*it];
        }

        // Insert the new child.
        YCHECK(impl->ChildToIndex().insert(MakePair(childId, beforeIndex)).second);
        list.insert(list.begin() + beforeIndex, childId);
    }

    AttachChild(childImpl);

    SetModified();
}

bool TListNodeProxy::RemoveChild(int index)
{
    auto* impl = LockThisTypedImpl(ELockMode::Exclusive, true);
    auto& list = impl->IndexToChild();

    if (index < 0 || index >= list.size()) {
        return false;
    }

    auto childProxy = GetProxy(list[index]);
    auto* childImpl = LockImpl(childProxy->GetId());

    // Update the indices.
    for (auto it = list.begin() + index + 1; it != list.end(); ++it) {
        --impl->ChildToIndex()[*it];
    }

    // Remove the child.
    list.erase(list.begin() + index);
    YCHECK(impl->ChildToIndex().erase(childProxy->GetId()));
    DetachChild(childImpl, true);

    SetModified();
    return true;
}

void TListNodeProxy::RemoveChild(INodePtr child)
{
    int index = GetChildIndex(child);
    YCHECK(RemoveChild(index));
}

void TListNodeProxy::ReplaceChild(INodePtr oldChild, INodePtr newChild)
{
    if (oldChild == newChild)
        return;

    auto* impl = LockThisTypedImpl();

    auto oldChildId = GetNodeId(oldChild);
    auto* oldChildImpl = LockImpl(oldChildId);

    auto newChildId = GetNodeId(newChild);
    auto* newChildImpl = LockImpl(newChildId);

    auto it = impl->ChildToIndex().find(oldChildId);
    YASSERT(it != impl->ChildToIndex().end());

    int index = it->second;

    DetachChild(oldChildImpl, true);

    impl->IndexToChild()[index] = newChildId;
    impl->ChildToIndex().erase(it);
    YCHECK(impl->ChildToIndex().insert(MakePair(newChildId, index)).second);
    AttachChild(newChildImpl);

    SetModified();
}

int TListNodeProxy::GetChildIndex(IConstNodePtr child)
{
    const auto* impl = GetThisTypedImpl();

    auto childId = GetNodeId(child);

    auto it = impl->ChildToIndex().find(childId);
    YASSERT(it != impl->ChildToIndex().end());

    return it->second;
}

void TListNodeProxy::SetRecursive(const TYPath& path, INodePtr value)
{
    TListNodeMixin::SetRecursive(path, value);
}

IYPathService::TResolveResult TListNodeProxy::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    return TListNodeMixin::ResolveRecursive(path, context);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NCypressServer
} // namespace NYT

