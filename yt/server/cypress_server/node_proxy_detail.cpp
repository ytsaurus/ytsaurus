#include "stdafx.h"
#include "node_proxy_detail.h"
#include "cypress_traversing.h"
#include "helpers.h"

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/ypath/tokenizer.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

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
        Consume(ResourceUsage, Consumer);
        Result.Set(TError());
    }

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TCypressNodeBase* trunkNode)
    : TObjectProxyBase(bootstrap, trunkNode)
    , TypeHandler(typeHandler)
    , Bootstrap(bootstrap)
    , Transaction(transaction)
    , TrunkNode(trunkNode)
    , CachedNode(nullptr)
{
    YASSERT(typeHandler);
    YASSERT(bootstrap);
    YASSERT(trunkNode);
    YASSERT(trunkNode->IsTrunk());

    Logger = NLog::TLogger("Cypress");
}

INodeFactoryPtr TNontemplateCypressNodeProxyBase::CreateFactory() const
{
    const auto* impl = GetThisImpl();
    auto* account = impl->GetAccount();
    return New<TNodeFactory>(Bootstrap, Transaction, account);
}

IYPathResolverPtr TNontemplateCypressNodeProxyBase::GetResolver() const
{
    if (!CachedResolver) {
        auto cypressManager = Bootstrap->GetCypressManager();
        CachedResolver = cypressManager->CreateResolver(Transaction);
    }
    return CachedResolver;
}

NTransactionServer::TTransaction* TNontemplateCypressNodeProxyBase::GetTransaction() const 
{
    return Transaction;
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::GetTrunkNode() const 
{
    return TrunkNode;
}

ENodeType TNontemplateCypressNodeProxyBase::GetType() const 
{
    return TypeHandler->GetNodeType();
}

ICompositeNodePtr TNontemplateCypressNodeProxyBase::GetParent() const 
{
    auto* parent = GetThisImpl()->GetParent();
    return parent ? GetProxy(parent)->AsComposite() : nullptr;
}

void TNontemplateCypressNodeProxyBase::SetParent(ICompositeNodePtr parent)
{
    auto* impl = LockThisImpl();
    impl->SetParent(parent ? ToProxy(INodePtr(parent))->GetTrunkNode() : nullptr);
}

bool TNontemplateCypressNodeProxyBase::IsWriteRequest(NRpc::IServiceContextPtr context) const 
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Lock);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Create);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Copy);
    return TNodeBase::IsWriteRequest(context);
}

const IAttributeDictionary& TNontemplateCypressNodeProxyBase::Attributes() const 
{
    return TObjectProxyBase::Attributes();
}

IAttributeDictionary* TNontemplateCypressNodeProxyBase::MutableAttributes()
{
    return TObjectProxyBase::MutableAttributes();
}

TAsyncError TNontemplateCypressNodeProxyBase::GetSystemAttributeAsync(
    const Stroka& key, 
    NYson::IYsonConsumer* consumer)
{
    if (key == "recursive_resource_usage") {
        auto visitor = New<TResourceUsageVisitor>(Bootstrap, consumer);
        return visitor->Run(const_cast<TNontemplateCypressNodeProxyBase*>(this));
    }

    return TObjectProxyBase::GetSystemAttributeAsync(key, consumer);
}

bool TNontemplateCypressNodeProxyBase::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "account") {
        ValidateNoTransaction();

        auto securityManager = Bootstrap->GetSecurityManager();

        auto name = ConvertTo<Stroka>(value);
        auto* account = securityManager->FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account: %s", ~name);
        }

        ValidatePermission(account, EPermission::Use);

        auto* node = LockThisImpl();
        securityManager->SetAccount(node, account);

        return true;
    }

    return TObjectProxyBase::SetSystemAttribute(key, value);
}

TVersionedObjectId TNontemplateCypressNodeProxyBase::GetVersionedId() const 
{
    return TVersionedObjectId(Object->GetId(), GetObjectId(Transaction));
}

void TNontemplateCypressNodeProxyBase::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    const auto* node = GetThisImpl();
    bool hasKey = node->GetParent() && node->GetParent()->GetType() == ENodeType::Map;
    attributes->push_back(TAttributeInfo("parent_id", node->GetParent()));
    attributes->push_back("locks");
    attributes->push_back("lock_mode");
    attributes->push_back(TAttributeInfo("path", true, true));
    attributes->push_back(TAttributeInfo("key", hasKey, false));
    attributes->push_back("creation_time");
    attributes->push_back("modification_time");
    attributes->push_back("resource_usage");
    attributes->push_back(TAttributeInfo("recursive_resource_usage", true, true));
    attributes->push_back("account");
    TObjectProxyBase::ListSystemAttributes(attributes);
}

bool TNontemplateCypressNodeProxyBase::GetSystemAttribute(
    const Stroka& key,
    IYsonConsumer* consumer)
{
    const auto* node = GetThisImpl();
    const auto* trunkNode = node->GetTrunkNode();
    bool hasKey = GetParent() && GetParent()->GetType() == ENodeType::Map;

    if (key == "parent_id" && node->GetParent()) {
        BuildYsonFluently(consumer)
            .Value(node->GetParent()->GetId());
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

    if (hasKey && key == "key") {
        BuildYsonFluently(consumer)
            .Value(GetParent()->AsMap()->GetChildKey(this));
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

    if (key == "account") {
        BuildYsonFluently(consumer)
            .Value(node->GetAccount()->GetName());
        return true;
    }

    return TObjectProxyBase::GetSystemAttribute(key, consumer);
}

bool TNontemplateCypressNodeProxyBase::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Lock);
    DISPATCH_YPATH_SERVICE_METHOD(Create);
    DISPATCH_YPATH_SERVICE_METHOD(Copy);

    if (TNodeBase::DoInvoke(context)) {
        return true;
    }

    if (TObjectProxyBase::DoInvoke(context)) {
        return true;
    }

    return false;
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::GetImpl(TCypressNodeBase* trunkNode) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->GetVersionedNode(trunkNode, Transaction);
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::LockImpl(
    TCypressNodeBase* trunkNode,
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->LockVersionedNode(trunkNode, Transaction, request, recursive);
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::GetThisImpl()
{
    if (CachedNode) {
        return CachedNode;
    }
    auto* node = GetImpl(TrunkNode);
    if (node->GetTransaction() == Transaction) {
        CachedNode = node;
    }
    return node;
}

const TCypressNodeBase* TNontemplateCypressNodeProxyBase::GetThisImpl() const
{
    return const_cast<TNontemplateCypressNodeProxyBase*>(this)->GetThisImpl();
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::LockThisImpl(
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/)
{
    // NB: Cannot use |CachedNode| here.
    return LockImpl(TrunkNode, request, recursive);
}

ICypressNodeProxyPtr TNontemplateCypressNodeProxyBase::GetProxy(TCypressNodeBase* trunkNode) const
{
    auto cypressManager = Bootstrap->GetCypressManager();
    return cypressManager->GetVersionedNodeProxy(trunkNode, Transaction);
}

ICypressNodeProxy* TNontemplateCypressNodeProxyBase::ToProxy(INodePtr node)
{
    return dynamic_cast<ICypressNodeProxy*>(~node);
}

const ICypressNodeProxy* TNontemplateCypressNodeProxyBase::ToProxy(IConstNodePtr node)
{
    return dynamic_cast<const ICypressNodeProxy*>(~node);
}

void TNontemplateCypressNodeProxyBase::AttachChild(TCypressNodeBase* child)
{
    child->SetParent(TrunkNode);

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->RefObject(child->GetTrunkNode());
}

void TNontemplateCypressNodeProxyBase::DetachChild(TCypressNodeBase* child, bool unref)
{
    child->SetParent(nullptr);
    if (unref) {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnrefObject(child->GetTrunkNode());
    }
}

TAutoPtr<IAttributeDictionary> TNontemplateCypressNodeProxyBase::DoCreateUserAttributes()
{
    return new TVersionedUserAttributeDictionary(
        TrunkNode,
        Transaction,
        Bootstrap);
}

void TNontemplateCypressNodeProxyBase::ValidatePermission(
    EPermissionCheckScope scope,
    EPermission permission)
{
    auto* node = GetThisImpl();
    ValidatePermission(node, scope, permission);
}

void TNontemplateCypressNodeProxyBase::ValidatePermission(
    TCypressNodeBase* node,
    EPermissionCheckScope scope,
    EPermission permission)
{
    if (scope & EPermissionCheckScope::This) {
        ValidatePermission(node, permission);
    }

    if (scope & EPermissionCheckScope::Parent) {
        ValidatePermission(node->GetParent(), permission);
    }

    if (scope & EPermissionCheckScope::Descendants) {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto* trunkNode = node->GetTrunkNode();
        auto descendants = cypressManager->ListSubtreeNodes(trunkNode, Transaction, false);
        FOREACH (auto* descendant, descendants) {
            ValidatePermission(descendant, permission);
        }
    }
}

void TNontemplateCypressNodeProxyBase::SetModified()
{
    auto cypressManager = Bootstrap->GetCypressManager();
    cypressManager->SetModified(TrunkNode, Transaction);
}

ICypressNodeProxyPtr TNontemplateCypressNodeProxyBase::ResolveSourcePath(const TYPath& path)
{
    auto sourceNode = this->GetResolver()->ResolvePath(path);
    return dynamic_cast<ICypressNodeProxy*>(~sourceNode);
}

bool TNontemplateCypressNodeProxyBase::CanHaveChildren() const
{
    return false;
}

void TNontemplateCypressNodeProxyBase::SetChild(const TYPath& path, INodePtr value, bool recursive)
{
    UNUSED(path);
    UNUSED(value);
    UNUSED(recursive);
    YUNREACHABLE();
}

TClusterResources TNontemplateCypressNodeProxyBase::GetResourceUsage() const 
{
    return TClusterResources(0, 1);
}

DEFINE_RPC_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Lock)
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

    ValidateTransaction();
    ValidatePermission(EPermissionCheckScope::This, mode == ELockMode::Snapshot ? EPermission::Read : EPermission::Write);

    auto cypressManager = Bootstrap->GetCypressManager();
    cypressManager->LockVersionedNode(TrunkNode, Transaction, mode);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Create)
{
    auto type = EObjectType(request->type());
    auto path = context->GetPath();

    context->SetRequestInfo("Type: %s", ~type.ToString());

    if (path.Empty()) {
        if (request->ignore_existing() && GetThisImpl()->GetType() == type) {
            ToProto(response->mutable_node_id(), GetId());
            context->Reply();
            return;
        }
        ThrowAlreadyExists(this);
    }

    if (!CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }

    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();
    auto securityManager = Bootstrap->GetSecurityManager();

    auto nodeHandler = cypressManager->FindHandler(type);
    if (!nodeHandler) {
        THROW_ERROR_EXCEPTION("Unknown object type: %s",
            ~type.ToString());
    }

    auto* schema = objectManager->GetSchema(type);
    securityManager->ValidatePermission(schema, EPermission::Create);
 
    auto* node = GetThisImpl();
    auto* account = node->GetAccount();
    ValidatePermission(account, EPermission::Use);

    auto attributes =
        request->has_node_attributes()
        ? FromProto(request->node_attributes())
        : CreateEphemeralAttributes();

    auto* newNode = cypressManager->CreateNode(
        nodeHandler,
        Transaction,
        account,
        ~attributes,
        request,
        response);

    auto newProxy = cypressManager->GetVersionedNodeProxy(
        newNode->GetTrunkNode(),
        Transaction);

    SetChild(path, newProxy, request->recursive());

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Copy)
{
    auto sourcePath = request->source_path();
    auto targetPath = context->GetPath();

    context->SetRequestInfo("SourcePath: %s", ~sourcePath);

    auto sourceProxy = ResolveSourcePath(sourcePath);
    if (sourceProxy->GetId() == GetId()) {
        THROW_ERROR_EXCEPTION("Cannot copy a node to its child");
    }

    if (!CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }   

    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();
    auto securityManager = Bootstrap->GetSecurityManager();

    auto* trunkSourceImpl = sourceProxy->GetTrunkNode();
    auto* sourceImpl = const_cast<TCypressNodeBase*>(GetImpl(trunkSourceImpl));

    auto* trunkDestImpl = GetTrunkNode();

    ValidatePermission(
        trunkSourceImpl,
        EPermissionCheckScope(EPermissionCheckScope::This | EPermissionCheckScope::Descendants),
        EPermission::Read);

    auto type = sourceImpl->GetType();
    auto* schema = objectManager->GetSchema(type);
    // TODO(babenko): also check descendant node types
    securityManager->ValidatePermission(schema, EPermission::Create);

    TCloneContext cloneContext;
    cloneContext.Account = trunkDestImpl->GetAccount();
    cloneContext.Transaction = Transaction;

    auto* clonedImpl = cypressManager->CloneNode(sourceImpl, cloneContext);
    auto* clonedTrunkImpl = clonedImpl->GetTrunkNode();
    auto clonedProxy = GetProxy(clonedTrunkImpl);

    SetChild(targetPath, clonedProxy, false);

    ToProto(response->mutable_object_id(), clonedTrunkImpl->GetId());

    context->Reply();
}

TAccessControlDescriptor* TNontemplateCypressNodeProxyBase::FindThisAcd()
{
    auto securityManager = Bootstrap->GetSecurityManager();
    auto* node = GetThisImpl();
    return securityManager->FindAcd(node);
}

////////////////////////////////////////////////////////////////////////////////

TNontemplateCompositeCypressNodeProxyBase::TNontemplateCompositeCypressNodeProxyBase(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
    : TNontemplateCypressNodeProxyBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

TIntrusivePtr<const NYTree::ICompositeNode> TNontemplateCompositeCypressNodeProxyBase::AsComposite() const 
{
    return this;
}

TIntrusivePtr<NYTree::ICompositeNode> TNontemplateCompositeCypressNodeProxyBase::AsComposite()
{
    return this;
}

void TNontemplateCompositeCypressNodeProxyBase::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    attributes->push_back("count");
    TNontemplateCypressNodeProxyBase::ListSystemAttributes(attributes);
}

bool TNontemplateCompositeCypressNodeProxyBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "count") {
        NYTree::BuildYsonFluently(consumer)
            .Value(this->GetChildCount());
        return true;
    }

    return TNontemplateCypressNodeProxyBase::GetSystemAttribute(key, consumer);
}

bool TNontemplateCompositeCypressNodeProxyBase::CanHaveChildren() const 
{
    return true;
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
    auto objectManager = Bootstrap->GetObjectManager();
    auto cypressManager = Bootstrap->GetCypressManager();
    auto handler = cypressManager->GetHandler(type);

    auto* node = cypressManager->CreateNode(
        handler,
        Transaction,
        Account,
        nullptr,
        nullptr,
        nullptr);
    auto* trunkNode = node->GetTrunkNode();

    objectManager->RefObject(trunkNode);
    CreatedNodes.push_back(trunkNode);

    return cypressManager->GetVersionedNodeProxy(trunkNode, Transaction);
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
        auto* child = pair.second;
        DoRemoveChild(impl, key, child);
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

std::vector< std::pair<Stroka, INodePtr> > TMapNodeProxy::GetChildren() const
{
    auto keyToChild = GetMapNodeChildren(Bootstrap, TrunkNode, Transaction);

    std::vector< std::pair<Stroka, INodePtr> > result;
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
    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();
    auto* childImpl = LockImpl(trunkChildImpl);

    impl->KeyToChild()[key] = trunkChildImpl;
    YCHECK(impl->ChildToKey().insert(std::make_pair(trunkChildImpl, key)).second);
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
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    DoRemoveChild(impl, key, childImpl);

    SetModified();

    return true;
}

void TMapNodeProxy::RemoveChild(INodePtr child)
{
    auto key = GetChildKey(child);
    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();

    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);
    auto* impl = LockThisTypedImpl(TLockRequest::SharedChild(key));
    DoRemoveChild(impl, key, childImpl);

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

bool TMapNodeProxy::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    return TBase::DoInvoke(context);
}

void TMapNodeProxy::SetChild(const TYPath& path, INodePtr value, bool recursive)
{
    TMapNodeMixin::SetChild(path, value, recursive);
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
    TCypressNodeBase* childImpl)
{
    auto* trunkChildImpl = childImpl->GetTrunkNode();
    auto& keyToChild = impl->KeyToChild();
    auto& childToKey = impl->ChildToKey();
    if (Transaction) {
        auto it = keyToChild.find(key);
        if (it == keyToChild.end()) {
            // TODO(babenko): remove cast when GCC supports native nullptr
            YCHECK(keyToChild.insert(std::make_pair(key, (TCypressNodeBase*) nullptr)).second);
            DetachChild(childImpl, false);
        } else {
            it->second = nullptr;
            YCHECK(childToKey.erase(trunkChildImpl) == 1);
            DetachChild(childImpl, true);
        }
    } else {
        YCHECK(keyToChild.erase(key) == 1);
        YCHECK(childToKey.erase(trunkChildImpl) == 1);
        DetachChild(childImpl, true);
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

    // Lock children and collect impls.
    std::vector<TCypressNodeBase*> children;
    FOREACH (auto* child, impl->IndexToChild()) {
        children.push_back(LockImpl(child));
    }

    // Detach children.
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
    FOREACH (auto* child, indexToChild) {
        result.push_back(GetProxy(child));
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
    auto* impl = LockThisTypedImpl();
    auto& list = impl->IndexToChild();

    if (index < 0 || index >= list.size()) {
        return false;
    }

    auto* trunkChildImpl = list[index];
    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);

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

void TListNodeProxy::SetChild(const TYPath& path, INodePtr value, bool recursive)
{
    TListNodeMixin::SetChild(path, value, recursive);
}

IYPathService::TResolveResult TListNodeProxy::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    return TListNodeMixin::ResolveRecursive(path, context);
}

////////////////////////////////////////////////////////////////////////////////

TLinkNodeProxy::TLinkNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TLinkNode* trunkNode)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

IYPathService::TResolveResult TLinkNodeProxy::Resolve(
    const TYPath& path,
    IServiceContextPtr context)
{
    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Ampersand:
            return TBase::Resolve(tokenizer.GetSuffix(), context);
        case NYPath::ETokenType::EndOfStream:
            // NB: Always handle RemoveThis locally.
            return context->GetVerb() == "Remove"
                   ? TResolveResult::Here(path)
                   : TResolveResult::There(GetTargetService(), path);
        default:
            return TResolveResult::There(GetTargetService(), path);
    }
}

void TLinkNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    TBase::ListSystemAttributes(attributes);
    attributes->push_back("target_id");
}

bool TLinkNodeProxy::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "target_id") {
        const auto* impl = GetThisTypedImpl();
        BuildYsonFluently(consumer)
            .Value(impl->GetTargetId());
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}


bool TLinkNodeProxy::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "target_id") {
        auto targetId = ConvertTo<TObjectId>(value);
        auto* impl = LockThisTypedImpl();
        impl->SetTargetId(targetId);
        return true;
    }

    return TBase::SetSystemAttribute(key, value);
}

IYPathServicePtr TLinkNodeProxy::GetTargetService() const
{
    auto objectManager = Bootstrap->GetObjectManager();
    const auto* impl = GetThisTypedImpl();
    const auto& targetId = impl->GetTargetId();
    auto* target = objectManager->FindObject(targetId);
    if (!target) {
        THROW_ERROR_EXCEPTION("Link target does not exist: %s", ~ToString(targetId));
    }
    return objectManager->GetProxy(target, Transaction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

