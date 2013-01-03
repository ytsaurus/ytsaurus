#include "stdafx.h"
#include "node_proxy_detail.h"
#include "cypress_traversing.h"
#include "helpers.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

TVersionedUserAttributeDictionary::TVersionedUserAttributeDictionary(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction,
    TBootstrap* bootstrap)
    : TrunkNode(trunkNode)
    , Transaction(transaction)
    , Bootstrap(bootstrap)
{ }

std::vector<Stroka> TVersionedUserAttributeDictionary::List() const 
{
    auto keyToAttribute = GetNodeAttributes(Bootstrap, TrunkNode, Transaction);
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
        TVersionedObjectId versionedId(TrunkNode->GetId(), GetObjectId(transaction));
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
        TrunkNode,
        Transaction,
        TLockRequest::SharedAttribute(key));
    auto versionedId = node->GetVersionedId();

    auto* userAttributes = objectManager->FindAttributes(versionedId);
    if (!userAttributes) {
        userAttributes = objectManager->CreateAttributes(versionedId);
    }

    userAttributes->Attributes()[key] = value;

    cypressManager->SetModified(TrunkNode, Transaction);
}

bool TVersionedUserAttributeDictionary::Remove(const Stroka& key)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto objectManager = Bootstrap->GetObjectManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    std::reverse(transactions.begin(), transactions.end());

    const TTransaction* containingTransaction = nullptr;
    bool contains = false;
    FOREACH (const auto* transaction, transactions) {
        TVersionedObjectId versionedId(TrunkNode->GetId(), GetObjectId(transaction));
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
        TrunkNode,
        Transaction,
        TLockRequest::SharedAttribute(key));
    auto versionedId = node->GetVersionedId();

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

    cypressManager->SetModified(TrunkNode, Transaction);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TResourceUsageVisitor
    : public ICypressNodeVisitor
{
public:
    explicit TResourceUsageVisitor(NCellMaster::TBootstrap* bootstrap, IYsonConsumer* consumer)
        : Bootstrap(bootstrap)
        , Consumer(consumer)
        , Result(NewPromise<TError>())
    { }

    TAsyncError Run(ICypressNodeProxyPtr rootNode)
    {
        TraverseCypress(Bootstrap, rootNode, this);
        return Result;
    }

private:
    NCellMaster::TBootstrap* Bootstrap;
    IYsonConsumer* Consumer;

    TPromise<TError> Result;
    TClusterResources ResourceUsage;

    virtual void OnNode(ICypressNodeProxyPtr node) override
    {
        ResourceUsage += node->GetResourceUsage();
    }

    virtual void OnError(const TError& error) override
    {
        auto wrappedError = TError("Error computing recursive resource usage")
            << error;
        Result.Set(wrappedError);
    }

    virtual void OnCompleted() override
    {
        Serialize(ResourceUsage, Consumer);
        Result.Set(TError());
    }

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCypressNodeProxyNontemplateBase::TCypressNodeProxyNontemplateBase(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TCypressNodeBase* trunkNode)
    : TObjectProxyBase(bootstrap, trunkNode)
    , TypeHandler(typeHandler)
    , Bootstrap(bootstrap)
    , Transaction(transaction)
    , TrunkNode(trunkNode)
{
    YASSERT(typeHandler);
    YASSERT(bootstrap);
    YASSERT(trunkNode);
    YASSERT(trunkNode->IsTrunk());

    Logger = NLog::TLogger("Cypress");
}

INodeFactoryPtr TCypressNodeProxyNontemplateBase::CreateFactory() const
{
    const auto* impl = GetThisImpl();
    auto* account = impl->GetAccount();
    return New<TNodeFactory>(Bootstrap, Transaction, account);
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

TCypressNodeBase* TCypressNodeProxyNontemplateBase::GetTrunkNode() const 
{
    return TrunkNode;
}

ENodeType TCypressNodeProxyNontemplateBase::GetType() const 
{
    return TypeHandler->GetNodeType();
}

ICompositeNodePtr TCypressNodeProxyNontemplateBase::GetParent() const 
{
    auto* parent = GetThisImpl()->GetParent();
    return parent ? GetProxy(parent)->AsComposite() : nullptr;
}

void TCypressNodeProxyNontemplateBase::SetParent(ICompositeNodePtr parent)
{
    auto* impl = LockThisImpl();
    impl->SetParent(parent ? ToProxy(INodePtr(parent))->GetTrunkNode() : nullptr);
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

TAsyncError TCypressNodeProxyNontemplateBase::GetSystemAttributeAsync(
    const Stroka& key, 
    NYson::IYsonConsumer* consumer) const
{
    if (key == "recursive_resource_usage") {
        auto visitor = New<TResourceUsageVisitor>(Bootstrap, consumer);
        return visitor->Run(const_cast<TCypressNodeProxyNontemplateBase*>(this));
    }

    return TObjectProxyBase::GetSystemAttributeAsync(key, consumer);
}

bool TCypressNodeProxyNontemplateBase::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "account") {
        if (Transaction) {
            THROW_ERROR_EXCEPTION("Attribute cannot be altered inside transaction");
        }

        auto securityManager = Bootstrap->GetSecurityManager();

        auto name = ConvertTo<Stroka>(value);
        auto* account = securityManager->FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account: %s", ~name);
        }

        auto* node = GetThisMutableImpl();
        securityManager->SetAccount(node, account);

        return true;
    }

    return TObjectProxyBase::SetSystemAttribute(key, value);
}

TVersionedObjectId TCypressNodeProxyNontemplateBase::GetVersionedId() const 
{
    return TVersionedObjectId(Object->GetId(), GetObjectId(Transaction));
}

void TCypressNodeProxyNontemplateBase::ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const 
{
    attributes->push_back("parent_id");
    attributes->push_back("locks");
    attributes->push_back("lock_mode");
    attributes->push_back(TAttributeInfo("path", true, true));
    attributes->push_back("creation_time");
    attributes->push_back("modification_time");
    attributes->push_back("resource_usage");
    attributes->push_back(TAttributeInfo("recursive_resource_usage", true, true));
    const auto* node = GetThisImpl();
    attributes->push_back("account");
    TObjectProxyBase::ListSystemAttributes(attributes);
}

bool TCypressNodeProxyNontemplateBase::GetSystemAttribute(
    const Stroka& key,
    IYsonConsumer* consumer) const 
{
    const auto* node = GetThisImpl();
    const auto* trunkNode = node->GetTrunkNode();

    if (key == "parent_id") {
        BuildYsonFluently(consumer)
            .Value(node->GetParent()->GetId().ToString());
        return true;
    }

    if (key == "locks") {
        BuildYsonFluently(consumer)
            .DoListFor(trunkNode->Locks(), [=] (TFluentList fluent, const TCypressNodeBase::TLockMap::value_type& pair) {
                fluent.Item()
                    .BeginMap()
                    .Item("mode").Value(pair.second.Mode)
                    .Item("transaction_id").Value(pair.first->GetId())
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
            .Value(FormatEnum(node->GetLockMode()));
        return true;
    }

    if (key == "path") {
        BuildYsonFluently(consumer)
            .Value(GetPath());
        return true;
    }

    if (key == "creation_time") {
        BuildYsonFluently(consumer)
            .Value(node->GetCreationTime().ToString());
        return true;
    }

    if (key == "modification_time") {
        BuildYsonFluently(consumer)
            .Value(node->GetModificationTime().ToString());
        return true;
    }

    if (key == "resource_usage") {
        BuildYsonFluently(consumer)
            .Value(GetResourceUsage());
        return true;
    }

    if (key == "account" && node->GetAccount()) {
        BuildYsonFluently(consumer)
            .Value(node->GetAccount()->GetName());
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

const TCypressNodeBase* TCypressNodeProxyNontemplateBase::GetImpl(TCypressNodeBase* trunkNode) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->GetVersionedNode(trunkNode, Transaction);
}

TCypressNodeBase* TCypressNodeProxyNontemplateBase::GetMutableImpl(TCypressNodeBase* trunkNode)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->GetVersionedNode(trunkNode, Transaction);
}

TCypressNodeBase* TCypressNodeProxyNontemplateBase::LockImpl(
    TCypressNodeBase* trunkNode,
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->LockVersionedNode(trunkNode, Transaction, request, recursive);
}

const TCypressNodeBase* TCypressNodeProxyNontemplateBase::GetThisImpl() const
{
    return GetImpl(TrunkNode);
}

TCypressNodeBase* TCypressNodeProxyNontemplateBase::GetThisMutableImpl()
{
    return GetMutableImpl(TrunkNode);
}

TCypressNodeBase* TCypressNodeProxyNontemplateBase::LockThisImpl(
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/)
{
    return LockImpl(TrunkNode, request, recursive);
}

ICypressNodeProxyPtr TCypressNodeProxyNontemplateBase::GetProxy(TCypressNodeBase* trunkNode) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->GetVersionedNodeProxy(trunkNode, Transaction);
}

ICypressNodeProxy* TCypressNodeProxyNontemplateBase::ToProxy(INodePtr node)
{
    return dynamic_cast<ICypressNodeProxy*>(~node);
}

const ICypressNodeProxy* TCypressNodeProxyNontemplateBase::ToProxy(IConstNodePtr node)
{
    return dynamic_cast<const ICypressNodeProxy*>(~node);
}

void TCypressNodeProxyNontemplateBase::AttachChild(TCypressNodeBase* child)
{
    child->SetParent(TrunkNode);

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->RefObject(child->GetTrunkNode());
}

void TCypressNodeProxyNontemplateBase::DetachChild(TCypressNodeBase* child, bool unref)
{
    child->SetParent(nullptr);
    if (unref) {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnrefObject(child->GetTrunkNode());
    }
}

TAutoPtr<IAttributeDictionary> TCypressNodeProxyNontemplateBase::DoCreateUserAttributes()
{
    return new TVersionedUserAttributeDictionary(
        TrunkNode,
        Transaction,
        Bootstrap);
}

void TCypressNodeProxyNontemplateBase::SetModified()
{
    auto cypressManager = Bootstrap->GetCypressManager();
    cypressManager->SetModified(TrunkNode, Transaction);
}

TClusterResources TCypressNodeProxyNontemplateBase::GetResourceUsage() const 
{
    return ZeroClusterResources();
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
    cypressManager->LockVersionedNode(TrunkNode, Transaction, mode);

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

////////////////////////////////////////////////////////////////////////////////

TNodeFactory::TNodeFactory(
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TAccount* account)
    : Bootstrap(bootstrap)
    , Transaction(transaction)
    , Account(account)
{
    YCHECK(bootstrap);
    YCHECK(account);
}

TNodeFactory::~TNodeFactory()
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (auto* node, CreatedNodes) {
        objectManager->UnrefObject(node);
    }
}

ICypressNodeProxyPtr TNodeFactory::DoCreate(EObjectType type)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto objectManager = Bootstrap->GetObjectManager();
    auto securityManager = Bootstrap->GetSecurityManager();
   
    auto handler = cypressManager->GetHandler(type);
  
    auto  node = handler->Create(Transaction, nullptr, nullptr);
    auto* node_ = ~node;

    cypressManager->RegisterNode(node, Transaction);

    if (!node_->GetAccount()) {
        securityManager->SetAccount(node_, Account);
    }
    
    objectManager->RefObject(node_);
    CreatedNodes.push_back(node_);

    return cypressManager->GetVersionedNodeProxy(node_, Transaction);
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
    TMapNode* trunkNode)
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
    auto keyToChild = GetMapNodeChildren(Bootstrap, TrunkNode, Transaction);

    // Take shared locks for children.
    typedef std::pair<Stroka, TCypressNodeBase*> TChild;
    std::vector<TChild> children;
    children.reserve(keyToChild.size());
    FOREACH (const auto& pair, keyToChild) {
        LockThisImpl(TLockRequest::SharedChild(pair.first));
        auto* childImpl = LockImpl(pair.second);
        children.push_back(std::make_pair(pair.first, childImpl));
    }

    // Insert tombstones (if in transaction).
    FOREACH (const auto& pair, children) {
        const auto& key = pair.first;
        auto* node = pair.second;
        auto* trunkNode = node->GetTrunkNode();
        DoRemoveChild(impl, key, trunkNode);
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
        TVersionedNodeId versionedId(Object->GetId(), GetObjectId(currentTransaction));
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
    auto keyToChild = GetMapNodeChildren(Bootstrap, TrunkNode, Transaction);

    std::vector< TPair<Stroka, INodePtr> > result;
    result.reserve(keyToChild.size());
    FOREACH (const auto& pair, keyToChild) {
        result.push_back(std::make_pair(pair.first, GetProxy(pair.second)));
    }

    return result;
}

std::vector<Stroka> TMapNodeProxy::GetKeys() const
{
    auto keyToChild = GetMapNodeChildren(Bootstrap, TrunkNode, Transaction);

    std::vector<Stroka> result;
    FOREACH (const auto& pair, keyToChild) {
        result.push_back(pair.first);
    }

    return result;
}

INodePtr TMapNodeProxy::FindChild(const Stroka& key) const
{
    auto* childTrunkNode = FindMapNodeChild(Bootstrap, TrunkNode, Transaction, key);
    return childTrunkNode ? GetProxy(childTrunkNode) : nullptr;
}

bool TMapNodeProxy::AddChild(INodePtr child, const Stroka& key)
{
    YASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    auto* childImpl = LockImpl(ToProxy(child)->GetTrunkNode());

    impl->KeyToChild()[key] = childImpl->GetTrunkNode();
    YCHECK(impl->ChildToKey().insert(std::make_pair(childImpl->GetTrunkNode(), key)).second);
    ++impl->ChildCountDelta();

    AttachChild(childImpl);

    SetModified();

    return true;
}

bool TMapNodeProxy::RemoveChild(const Stroka& key)
{
    auto* trunkChildImpl = FindMapNodeChild(Bootstrap, TrunkNode, Transaction, key);
    if (!trunkChildImpl) {
        return false;
    }

    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);
    auto* childTrunkImpl = childImpl->GetTrunkNode();

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    
    DoRemoveChild(impl, key, childTrunkImpl);
    
    SetModified();

    return true;
}

void TMapNodeProxy::RemoveChild(INodePtr child)
{
    auto key = GetChildKey(child);
    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();

    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);
    auto* childTrunkImpl = childImpl->GetTrunkNode();

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    
    DoRemoveChild(impl, key, childTrunkImpl);

    SetModified();
}

void TMapNodeProxy::ReplaceChild(INodePtr oldChild, INodePtr newChild)
{
    if (oldChild == newChild)
        return;

    auto key = GetChildKey(oldChild);

    auto* oldTrunkChildImpl = ToProxy(oldChild)->GetTrunkNode();
    auto* oldChildImpl = LockImpl(oldTrunkChildImpl, ELockMode::Exclusive, true);

    
    auto* newTrunkChildImpl = ToProxy(newChild)->GetTrunkNode();
    auto* newChildImpl = LockImpl(newTrunkChildImpl);

    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));

    auto& keyToChild = impl->KeyToChild();
    auto& childToKey = impl->ChildToKey();

    bool ownsOldChild = keyToChild.find(key) != keyToChild.end();
    DetachChild(oldChildImpl, ownsOldChild);

    keyToChild[key] = newTrunkChildImpl;
    YCHECK(childToKey.insert(std::make_pair(newTrunkChildImpl, key)).second);    
    AttachChild(newChildImpl);

    SetModified();
}

Stroka TMapNodeProxy::GetChildKey(IConstNodePtr child)
{
    auto cypressManager = Bootstrap->GetCypressManager();
    auto transactionManager = Bootstrap->GetTransactionManager();

    auto transactions = transactionManager->GetTransactionPath(Transaction);
    // NB: Use the latest key, don't reverse transactions.
    
    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();

    FOREACH (const auto* currentTransaction, transactions) {
        TVersionedNodeId versionedId(Object->GetId(), GetObjectId(currentTransaction));
        const auto* node = cypressManager->FindNode(versionedId);
        if (node) {
            const auto* mapNode = static_cast<const TMapNode*>(node);
            auto it = mapNode->ChildToKey().find(trunkChildImpl);
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

void TMapNodeProxy::DoRemoveChild(
    TMapNode* impl,
    const Stroka& key,
    TCypressNodeBase* trunkChildImpl)
{
    YASSERT(trunkChildImpl->IsTrunk());

    auto& keyToChild = impl->KeyToChild();
    auto& childToKey = impl->ChildToKey();
    if (Transaction) {
        auto it = keyToChild.find(key);
        if (it == keyToChild.end()) {
            // TODO(babenko): remove cast when GCC supports native nullptr
            YCHECK(keyToChild.insert(std::make_pair(key, (TCypressNodeBase*) nullptr)).second);
            DetachChild(trunkChildImpl, false);
        } else {
            it->second = nullptr;
            YCHECK(childToKey.erase(trunkChildImpl) == 1);
            DetachChild(trunkChildImpl, true);
        }
    } else {
        YCHECK(keyToChild.erase(key) == 1);
        YCHECK(childToKey.erase(trunkChildImpl) == 1);
        DetachChild(trunkChildImpl, true);
    }
    --impl->ChildCountDelta();
}

////////////////////////////////////////////////////////////////////////////////

TListNodeProxy::TListNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TListNode* trunkNode)
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
    std::vector<TCypressNodeBase*> children;
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
    return index >= 0 && index < indexToChild.size() ? GetProxy(indexToChild[index]) : nullptr;
}

void TListNodeProxy::AddChild(INodePtr child, int beforeIndex /*= -1*/)
{
    auto* impl = LockThisTypedImpl();
    auto& list = impl->IndexToChild();

    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();
    auto* childImpl = LockImpl(trunkChildImpl);

    if (beforeIndex < 0) {
        YCHECK(impl->ChildToIndex().insert(std::make_pair(trunkChildImpl, list.size())).second);
        list.push_back(trunkChildImpl);
    } else {
        // Update indices.
        for (auto it = list.begin() + beforeIndex; it != list.end(); ++it) {
            ++impl->ChildToIndex()[*it];
        }

        // Insert the new child.
        YCHECK(impl->ChildToIndex().insert(std::make_pair(trunkChildImpl, beforeIndex)).second);
        list.insert(list.begin() + beforeIndex, trunkChildImpl);
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

    auto* trunkChildImpl = list[index];
    auto* childImpl = LockImpl(trunkChildImpl);

    // Update the indices.
    for (auto it = list.begin() + index + 1; it != list.end(); ++it) {
        --impl->ChildToIndex()[*it];
    }

    // Remove the child.
    list.erase(list.begin() + index);
    YCHECK(impl->ChildToIndex().erase(trunkChildImpl));
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

    auto* oldTrunkChildImpl = ToProxy(oldChild)->GetTrunkNode();
    auto* oldChildImpl = LockImpl(oldTrunkChildImpl);

    auto* newTrunkChildImpl = ToProxy(newChild)->GetTrunkNode();
    auto* newChildImpl = LockImpl(newTrunkChildImpl);

    auto it = impl->ChildToIndex().find(oldTrunkChildImpl);
    YASSERT(it != impl->ChildToIndex().end());

    int index = it->second;

    DetachChild(oldChildImpl, true);

    impl->IndexToChild()[index] = newTrunkChildImpl;
    impl->ChildToIndex().erase(it);
    YCHECK(impl->ChildToIndex().insert(std::make_pair(newTrunkChildImpl, index)).second);
    AttachChild(newChildImpl);

    SetModified();
}

int TListNodeProxy::GetChildIndex(IConstNodePtr child)
{
    const auto* impl = GetThisTypedImpl();

    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();

    auto it = impl->ChildToIndex().find(trunkChildImpl);
    YCHECK(it != impl->ChildToIndex().end());

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

