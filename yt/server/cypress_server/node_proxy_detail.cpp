#include "stdafx.h"
#include "node_proxy_detail.h"
#include "cypress_traversing.h"
#include "helpers.h"
#include "private.h"

#include <core/misc/string.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <core/ytree/ypath_proxy.h> // XXX(babenko): improve
#include <core/ytree/ypath_detail.h>
#include <core/ytree/node_detail.h>
#include <core/ytree/convert.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/fluent.h>
#include <core/ytree/ypath_client.h>
#include <core/ytree/exception_helpers.h>

#include <core/ypath/tokenizer.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/object_client/object_service_proxy.h> // XXX(babenko): improve

#include <server/cell_master/config.h>
#include <server/cell_master/bootstrap.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary
    : public IAttributeDictionary
{
public:
    explicit TCustomAttributeDictionary(TNontemplateCypressNodeProxyBase* proxy)
        : Proxy_(proxy)
    { }

    virtual std::vector<Stroka> List() const override
    {
        auto keys = ListNodeAttributes(
            Proxy_->Bootstrap_,
            Proxy_->TrunkNode,
            Proxy_->Transaction);
        return std::vector<Stroka>(keys.begin(), keys.end());
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& name) const override
    {
        auto cypressManager = Proxy_->Bootstrap_->GetCypressManager();
        auto originators = cypressManager->GetNodeOriginators(Proxy_->GetTransaction(), Proxy_->GetTrunkNode());
        for (const auto* node : originators) {
            const auto* userAttributes = node->GetAttributes();
            if (userAttributes) {
                auto it = userAttributes->Attributes().find(name);
                if (it != userAttributes->Attributes().end()) {
                    return it->second;
                }
            }
        }

        return Null;
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        auto cypressManager = Proxy_->Bootstrap_->GetCypressManager();

        auto oldValue = FindYson(key);
        Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, value);

        auto* node = cypressManager->LockNode(
            Proxy_->TrunkNode,
            Proxy_->Transaction,
            TLockRequest::SharedAttribute(key));

        auto* userAttributes = node->GetMutableAttributes();
        userAttributes->Attributes()[key] = value;

        cypressManager->SetModified(Proxy_->TrunkNode, Proxy_->Transaction);
    }

    virtual bool Remove(const Stroka& key) override
    {
        auto cypressManager = Proxy_->Bootstrap_->GetCypressManager();
        auto originators = cypressManager->GetNodeReverseOriginators(Proxy_->GetTransaction(), Proxy_->GetTrunkNode());

        auto oldValue = FindYson(key);
        Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, Null);

        const TTransaction* containingTransaction = nullptr;
        bool contains = false;
        for (const auto* node : originators) {
            const auto* userAttributes = node->GetAttributes();
            if (userAttributes) {
                auto it = userAttributes->Attributes().find(key);
                if (it != userAttributes->Attributes().end()) {
                    contains = it->second.HasValue();
                    if (contains) {
                        containingTransaction = node->GetTransaction();
                    }
                    break;
                }
            }
        }

        if (!contains) {
            return false;
        }

        auto* node = cypressManager->LockNode(
            Proxy_->TrunkNode,
            Proxy_->Transaction,
            TLockRequest::SharedAttribute(key));

        auto* userAttributes = node->GetMutableAttributes();
        if (containingTransaction == Proxy_->Transaction) {
            YCHECK(userAttributes->Attributes().erase(key) == 1);
        } else {
            YCHECK(!containingTransaction);
            userAttributes->Attributes()[key] = Null;
        }

        cypressManager->SetModified(Proxy_->TrunkNode, Proxy_->Transaction);
        return true;
    }

protected:
    TNontemplateCypressNodeProxyBase* const Proxy_;

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeProxyBase::TResourceUsageVisitor
    : public ICypressNodeVisitor
{
public:
    explicit TResourceUsageVisitor(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TPromise<TYsonString> Run(ICypressNodeProxyPtr rootNode)
    {
        TraverseCypress(Bootstrap_, rootNode, this);
        return Promise_;
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    TPromise<TYsonString> Promise_ = NewPromise<TYsonString>();
    TClusterResources ResourceUsage_;


    virtual void OnNode(ICypressNodeProxyPtr proxy) override
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetVersionedNode(proxy->GetTrunkNode(), proxy->GetTransaction());
        auto handler = cypressManager->GetHandler(node);
        ResourceUsage_ += handler->GetTotalResourceUsage(node);
    }

    virtual void OnError(const TError& error) override
    {
        auto wrappedError = TError("Error computing recursive resource usage")
            << error;
        Promise_.Set(wrappedError);
    }

    virtual void OnCompleted() override
    {
        Promise_.Set(ConvertToYsonString(ResourceUsage_));
    }

};

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    TTransaction* transaction,
    TCypressNodeBase* trunkNode)
    : TObjectProxyBase(bootstrap, trunkNode)
    , TypeHandler(typeHandler)
    , Config(Bootstrap_->GetConfig()->CypressManager)
    , Transaction(transaction)
    , TrunkNode(trunkNode)
{
    YASSERT(typeHandler);
    YASSERT(trunkNode);
    YASSERT(trunkNode->IsTrunk());
}

INodeFactoryPtr TNontemplateCypressNodeProxyBase::CreateFactory() const
{
    return CreateCypressFactory(false);
}

ICypressNodeFactoryPtr TNontemplateCypressNodeProxyBase::CreateCypressFactory(
    bool preserveAccount) const
{
    const auto* impl = GetThisImpl();
    auto* account = impl->GetAccount();
    auto cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->CreateNodeFactory(
        Transaction,
        account,
        preserveAccount);
}

INodeResolverPtr TNontemplateCypressNodeProxyBase::GetResolver() const
{
    if (!CachedResolver) {
        auto cypressManager = Bootstrap_->GetCypressManager();
        CachedResolver = cypressManager->CreateResolver(Transaction);
    }
    return CachedResolver;
}

TTransaction* TNontemplateCypressNodeProxyBase::GetTransaction() const
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

const IAttributeDictionary& TNontemplateCypressNodeProxyBase::Attributes() const
{
    return TObjectProxyBase::Attributes();
}

IAttributeDictionary* TNontemplateCypressNodeProxyBase::MutableAttributes()
{
    return TObjectProxyBase::MutableAttributes();
}

TFuture<TYsonString> TNontemplateCypressNodeProxyBase::GetBuiltinAttributeAsync(const Stroka& key)
{
    if (key == "recursive_resource_usage") {
        auto visitor = New<TResourceUsageVisitor>(Bootstrap_);
        return visitor->Run(this);
    }

    auto asyncResult = GetExternalBuiltinAttributeAsync(key);
    if (asyncResult) {
        return asyncResult;
    }

    return TObjectProxyBase::GetBuiltinAttributeAsync(key);
}

TFuture<TYsonString> TNontemplateCypressNodeProxyBase::GetExternalBuiltinAttributeAsync(const Stroka& key)
{
    const auto* node = GetThisImpl();
    if (!node->IsExternal()) {
        return Null;
    }

    auto maybeDescriptor = FindBuiltinAttributeDescriptor(key);
    if (!maybeDescriptor) {
        return Null;
    }

    const auto& descriptor = *maybeDescriptor;
    if (!descriptor.External) {
        return Null;
    }

    auto cellTag = node->GetExternalCellTag();
    auto cellId = Bootstrap_->GetSecondaryCellId(cellTag);

    auto cellDirectory = Bootstrap_->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(cellId);

    // XXX(babenko): improve
    TObjectServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Bootstrap_->GetConfig()->ObjectManager->ForwardingRpcTimeout);

    auto versionedId = GetVersionedId();

    auto req = TYPathProxy::Get(FromObjectId(versionedId.ObjectId) + "/@" + key);
    SetTransactionId(req, versionedId.TransactionId);

    return proxy.Execute(req).Apply(BIND([=] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) {
        if (!rspOrError.IsOK()) {
            if (rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
                return TYsonString();
            }
            THROW_ERROR_EXCEPTION("Error requesting attribute %Qv of object %v from cell %v",
                key,
                versionedId,
                cellTag)
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();
        return TYsonString(rsp->value());
    }));
}

bool TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "account") {
        ValidateNoTransaction();
        
        auto securityManager = Bootstrap_->GetSecurityManager();

        auto name = ConvertTo<Stroka>(value);
        auto* account = securityManager->GetAccountByNameOrThrow(name);

        ValidatePermission(account, EPermission::Use);

        auto* node = LockThisImpl();
        if (node->GetAccount() != account) {
            account->ValidateResourceUsageIncrease(TClusterResources(0, 1, 0));
            securityManager->SetAccount(node, account);
        }

        return true;
    }

    return TObjectProxyBase::SetBuiltinAttribute(key, value);
}

TFuture<bool> TNontemplateCypressNodeProxyBase::CheckBuiltinAttributeExistsAsync(const Stroka& key)
{
    auto asyncResult = CheckExternalBuiltinAttributeExistsAsync(key);
    if (asyncResult) {
        return asyncResult;
    }

    return TObjectProxyBase::CheckBuiltinAttributeExistsAsync(key);
}

TFuture<bool> TNontemplateCypressNodeProxyBase::CheckExternalBuiltinAttributeExistsAsync(const Stroka& key)
{
    const auto* node = GetThisImpl();
    if (!node->IsExternal()) {
        return Null;
    }

    auto maybeDescriptor = FindBuiltinAttributeDescriptor(key);
    if (!maybeDescriptor) {
        return Null;
    }

    const auto& descriptor = *maybeDescriptor;
    if (!descriptor.External) {
        return Null;
    }

    auto cellTag = node->GetExternalCellTag();
    auto cellId = Bootstrap_->GetSecondaryCellId(cellTag);

    auto cellDirectory = Bootstrap_->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(cellId);

    // XXX(babenko): improve
    TObjectServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Bootstrap_->GetConfig()->ObjectManager->ForwardingRpcTimeout);

    auto versionedId = GetVersionedId();

    auto req = TYPathProxy::Exists(FromObjectId(versionedId.ObjectId) + "/@" + key);
    SetTransactionId(req, versionedId.TransactionId);

    return proxy.Execute(req).Apply(BIND([=] (const TYPathProxy::TErrorOrRspExistsPtr& rspOrError) {
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error checking attribute %Qv of object %v from cell %v",
                key,
                versionedId,
                cellTag)
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();
        return rsp->value();
    }));
}

TVersionedObjectId TNontemplateCypressNodeProxyBase::GetVersionedId() const
{
    return TVersionedObjectId(Object_->GetId(), GetObjectId(Transaction));
}

void TNontemplateCypressNodeProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TObjectProxyBase::ListSystemAttributes(descriptors);

    const auto* node = GetThisImpl();
    bool hasKey = NodeHasKey(Bootstrap_, node);
    bool isExternal = node->IsExternal();

    descriptors->push_back(TAttributeDescriptor("parent_id")
        .SetPresent(node->GetParent()));
    descriptors->push_back("external");
    descriptors->push_back(TAttributeDescriptor("external_cell_tag")
        .SetPresent(isExternal));
    descriptors->push_back("locks");
    descriptors->push_back("lock_mode");
    descriptors->push_back(TAttributeDescriptor("path")
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("key")
        .SetPresent(hasKey));
    descriptors->push_back("creation_time");
    descriptors->push_back("modification_time");
    descriptors->push_back("access_time");
    descriptors->push_back("access_counter");
    descriptors->push_back("revision");
    descriptors->push_back(TAttributeDescriptor("resource_usage")
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("recursive_resource_usage")
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("account")
        .SetReplicated(true)
        .SetWritePermission(EPermission::Administer));
    descriptors->push_back("user_attribute_keys");
}

bool TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(
    const Stroka& key,
    IYsonConsumer* consumer)
{
    const auto* node = GetThisImpl();
    const auto* trunkNode = node->GetTrunkNode();
    bool hasKey = NodeHasKey(Bootstrap_, node);
    bool isExternal = node->IsExternal();

    if (key == "parent_id" && node->GetParent()) {
        BuildYsonFluently(consumer)
            .Value(node->GetParent()->GetId());
        return true;
    }

    if (key == "external") {
        BuildYsonFluently(consumer)
            .Value(isExternal);
        return true;
    }

    if (key == "external_cell_tag" && isExternal) {
        BuildYsonFluently(consumer)
            .Value(node->GetExternalCellTag());
        return true;
    }

    if (key == "locks") {
        auto printLock = [=] (TFluentList fluent, const TLock* lock) {
            fluent.Item()
                .BeginMap()
                    .Item("id").Value(lock->GetId())
                    .Item("state").Value(lock->GetState())
                    .Item("transaction_id").Value(lock->GetTransaction()->GetId())
                    .Item("mode").Value(lock->Request().Mode)
                    .DoIf(lock->Request().ChildKey.HasValue(), [=] (TFluentMap fluent) {
                        fluent
                            .Item("child_key").Value(*lock->Request().ChildKey);
                    })
                    .DoIf(lock->Request().AttributeKey.HasValue(), [=] (TFluentMap fluent) {
                        fluent
                            .Item("attribute_key").Value(*lock->Request().AttributeKey);
                    })
                .EndMap();
        };

        BuildYsonFluently(consumer)
            .BeginList()
                .DoFor(trunkNode->AcquiredLocks(), printLock)
                .DoFor(trunkNode->PendingLocks(), printLock)
            .EndList();
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
            .Value(node->GetCreationTime());
        return true;
    }

    if (key == "modification_time") {
        BuildYsonFluently(consumer)
            .Value(node->GetModificationTime());
        return true;
    }

    if (key == "access_time") {
        BuildYsonFluently(consumer)
            .Value(trunkNode->GetAccessTime());
        return true;
    }
 
    if (key == "access_counter") {
        BuildYsonFluently(consumer)
            .Value(trunkNode->GetAccessCounter());
        return true;
    }

    if (key == "revision") {
        BuildYsonFluently(consumer)
            .Value(node->GetRevision());
        return true;
    }

    if (key == "resource_usage") {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto handler = cypressManager->GetHandler(node);
        BuildYsonFluently(consumer)
            .Value(handler->GetTotalResourceUsage(node));
        return true;
    }

    if (key == "account") {
        BuildYsonFluently(consumer)
            .Value(node->GetAccount()->GetName());
        return true;
    }

    if (key == "user_attribute_keys") {
        std::vector<TAttributeDescriptor> systemAttributes;
        ListSystemAttributes(&systemAttributes);

        auto customAttributes = GetCustomAttributes()->List();
        yhash_set<Stroka> customAttributesSet(customAttributes.begin(), customAttributes.end());

        for (const auto& attribute : systemAttributes) {
            if (attribute.Custom) {
                customAttributesSet.erase(attribute.Key);
            }
        }

        BuildYsonFluently(consumer)
            .Value(customAttributesSet);
        return true;
    }

    return TObjectProxyBase::GetBuiltinAttribute(key, consumer);
}

void TNontemplateCypressNodeProxyBase::BeforeInvoke(IServiceContextPtr context)
{
    AccessTrackingSuppressed = GetSuppressAccessTracking(context->RequestHeader());
    ModificationTrackingSuppressed = GetSuppressModificationTracking(context->RequestHeader());

    TObjectProxyBase::BeforeInvoke(std::move(context));
}

void TNontemplateCypressNodeProxyBase::AfterInvoke(IServiceContextPtr context)
{
    if (!AccessTrackingSuppressed) {
        SetAccessed();
    }

    TObjectProxyBase::AfterInvoke(std::move(context));
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

void TNontemplateCypressNodeProxyBase::RemoveSelf(
    TReqRemove* request,
    TRspRemove* response,
    TCtxRemovePtr context)
{
    auto* node = GetThisImpl();
    auto objectManager = Bootstrap_->GetObjectManager();
    if (objectManager->IsForeign(node)) {
        YCHECK(node->IsTrunk());
        YCHECK(node->AcquiredLocks().empty());
        YCHECK(node->GetObjectRefCounter() == 1);
        objectManager->UnrefObject(node);
    } else {
        TNodeBase::RemoveSelf(request, response, std::move(context));
    }
}

void TNontemplateCypressNodeProxyBase::GetAttribute(
    const TYPath& path,
    TReqGet* request,
    TRspGet* response,
    TCtxGetPtr context)
{
    SuppressAccessTracking();
    TObjectProxyBase::GetAttribute(path, request, response, context);
}

void TNontemplateCypressNodeProxyBase::ListAttribute(
    const TYPath& path,
    TReqList* request,
    TRspList* response,
    TCtxListPtr context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ListAttribute(path, request, response, context);
}

void TNontemplateCypressNodeProxyBase::ExistsSelf(
    TReqExists* request,
    TRspExists* response,
    TCtxExistsPtr context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ExistsSelf(request, response, context);
}

void TNontemplateCypressNodeProxyBase::ExistsRecursive(
    const TYPath& path,
    TReqExists* request,
    TRspExists* response,
    TCtxExistsPtr context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ExistsRecursive(path, request, response, context);
}

void TNontemplateCypressNodeProxyBase::ExistsAttribute(
    const TYPath& path,
    TReqExists* request,
    TRspExists* response,
    TCtxExistsPtr context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ExistsAttribute(path, request, response, context);
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::GetImpl(TCypressNodeBase* trunkNode) const
{
    auto cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->GetVersionedNode(trunkNode, Transaction);
}

TCypressNodeBase* TNontemplateCypressNodeProxyBase::LockImpl(
    TCypressNodeBase* trunkNode,
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/) const
{
    auto cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->LockNode(trunkNode, Transaction, request, recursive);
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
    CachedNode = nullptr;
    return LockImpl(TrunkNode, request, recursive);
}

ICypressNodeProxyPtr TNontemplateCypressNodeProxyBase::GetProxy(TCypressNodeBase* trunkNode) const
{
    auto cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->GetNodeProxy(trunkNode, Transaction);
}

ICypressNodeProxy* TNontemplateCypressNodeProxyBase::ToProxy(INodePtr node)
{
    return dynamic_cast<ICypressNodeProxy*>(node.Get());
}

const ICypressNodeProxy* TNontemplateCypressNodeProxyBase::ToProxy(IConstNodePtr node)
{
    return dynamic_cast<const ICypressNodeProxy*>(node.Get());
}

std::unique_ptr<IAttributeDictionary> TNontemplateCypressNodeProxyBase::DoCreateCustomAttributes()
{
    return std::unique_ptr<IAttributeDictionary>(new TCustomAttributeDictionary(this));
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
    if ((scope & EPermissionCheckScope::This) != EPermissionCheckScope::None) {
        ValidatePermission(node, permission);
    }

    if ((scope & EPermissionCheckScope::Parent) != EPermissionCheckScope::None) {
        ValidatePermission(node->GetParent(), permission);
    }

    if ((scope & EPermissionCheckScope::Descendants) != EPermissionCheckScope::None) {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = node->GetTrunkNode();
        auto descendants = cypressManager->ListSubtreeNodes(trunkNode, Transaction, false);
        for (auto* descendant : descendants) {
            ValidatePermission(descendant, permission);
        }
    }
}

void TNontemplateCypressNodeProxyBase::ValidateNotExternal()
{
    if (TrunkNode->IsExternal()) {
        THROW_ERROR_EXCEPTION("Operation cannot be performed at an external node");
    }
}

void TNontemplateCypressNodeProxyBase::SetModified()
{
    if (TrunkNode->IsAlive() && !ModificationTrackingSuppressed) {
        auto cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetModified(TrunkNode, Transaction);
    }
}

void TNontemplateCypressNodeProxyBase::SuppressModificationTracking()
{
    ModificationTrackingSuppressed = true;
}

void TNontemplateCypressNodeProxyBase::SetAccessed()
{
    if (TrunkNode->IsAlive()) {
        auto cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetAccessed(TrunkNode);
    }
}

void TNontemplateCypressNodeProxyBase::SuppressAccessTracking()
{
    AccessTrackingSuppressed = true;
}

bool TNontemplateCypressNodeProxyBase::CanHaveChildren() const
{
    return false;
}

void TNontemplateCypressNodeProxyBase::SetChildNode(
    INodeFactoryPtr /*factory*/,
    const TYPath& /*path*/,
    INodePtr /*value*/,
    bool /*recursive*/)
{
    YUNREACHABLE();
}

NLogging::TLogger TNontemplateCypressNodeProxyBase::CreateLogger() const
{
    return CypressServerLogger;
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Lock)
{
    DeclareMutating();

    auto mode = ELockMode(request->mode());
    bool waitable = request->waitable();

    auto lockRequest = TLockRequest(mode);

    if (request->has_child_key()) {
        if (mode != ELockMode::Shared) {
            THROW_ERROR_EXCEPTION("Only %Qlv locks are allowed on child keys, got %Qlv",
                ELockMode::Shared,
                mode);
        }
        lockRequest.ChildKey = request->child_key();
    }

    if (request->has_attribute_key()) {
        if (mode != ELockMode::Shared) {
            THROW_ERROR_EXCEPTION("Only %Qlv locks are allowed on attribute keys, got %Qlv",
                ELockMode::Shared,
                mode);
        }
        lockRequest.AttributeKey = request->attribute_key();
    }

    if (mode != ELockMode::Snapshot &&
        mode != ELockMode::Shared &&
        mode != ELockMode::Exclusive)
    {
        THROW_ERROR_EXCEPTION("Invalid lock mode %Qlv",
            mode);
    }

    context->SetRequestInfo("Mode: %v, Waitable: %v",
        mode,
        waitable);

    ValidateTransaction();
    ValidatePermission(
        EPermissionCheckScope::This,
        mode == ELockMode::Snapshot ? EPermission::Read : EPermission::Write);

    auto cypressManager = Bootstrap_->GetCypressManager();
    auto* lock = cypressManager->CreateLock(
        TrunkNode,
        Transaction,
        lockRequest,
        waitable);
    auto lockId = GetObjectId(lock);
    ToProto(response->mutable_lock_id(), lockId);

    context->SetResponseInfo("LockId: %v",
        lockId);

    context->Reply();

    const auto* node = GetThisImpl();
    if (node->IsExternal()) {
        PostToSecondaryMaster(context, node->GetExternalCellTag());
    }
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Create)
{
    DeclareMutating();

    auto type = EObjectType(request->type());
    auto ignoreExisting = request->ignore_existing();
    auto recursive = request->recursive();
    const auto& path = GetRequestYPath(context);

    context->SetRequestInfo("Type: %v, IgnoreExisting: %v, Recursive: %v",
        type,
        ignoreExisting,
        recursive);

    if (path.Empty()) {
        if (ignoreExisting && GetThisImpl()->GetType() == type) {
            ToProto(response->mutable_node_id(), GetId());
            context->Reply();
            return;
        }
        ThrowAlreadyExists(this);
    }

    if (!CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* node = GetThisImpl();
    auto* account = node->GetAccount();
    ValidatePermission(account, EPermission::Use);

    auto factory = CreateCypressFactory(false);

    auto attributes = request->has_node_attributes()
        ? FromProto(request->node_attributes())
        : std::unique_ptr<IAttributeDictionary>();

    auto newProxy = factory->CreateNode(
        type,
        attributes.get(),
        request,
        response);

    SetChildNode(factory, path, newProxy, request->recursive());

    factory->Commit();

    context->SetResponseInfo("NodeId: %v", newProxy->GetId());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Copy)
{
    DeclareMutating();

    auto sourcePath = request->source_path();
    bool preserveAccount = request->preserve_account();
    bool removeSource = request->remove_source();
    auto recursive = request->recursive();
    auto targetPath = GetRequestYPath(context);

    context->SetRequestInfo("SourcePath: %v, PreserveAccount: %v, RemoveSource: %v, Recursive: %v",
        sourcePath,
        preserveAccount,
        removeSource,
        recursive);

    auto ytreeSourceProxy = GetResolver()->ResolvePath(sourcePath);
    auto* sourceProxy = dynamic_cast<ICypressNodeProxy*>(ytreeSourceProxy.Get());
    YCHECK(sourceProxy);

    if (targetPath.empty()) {
        ThrowAlreadyExists(this);
    }

    if (!CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }

    auto* trunkSourceImpl = sourceProxy->GetTrunkNode();
    auto* sourceImpl = removeSource
        ? LockImpl(trunkSourceImpl, ELockMode::Exclusive, true)
        : GetImpl(trunkSourceImpl);

    if (IsParentOf(sourceImpl, GetThisImpl())) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to its descendant");
    }

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    ValidatePermission(
        trunkSourceImpl,
        EPermissionCheckScope::This | EPermissionCheckScope::Descendants,
        removeSource ? EPermission::Read : EPermission::Read | EPermission::Write);

    auto sourceParent = sourceProxy->GetParent();
    if (removeSource) {
        // Cf. TNodeBase::RemoveSelf
        if (!sourceParent) {
            ThrowCannotRemoveRoot();
        }

        ValidatePermission(sourceImpl, EPermissionCheckScope::This, EPermission::Remove);
        ValidatePermission(sourceImpl, EPermissionCheckScope::Descendants, EPermission::Remove);
        ValidatePermission(sourceImpl, EPermissionCheckScope::Parent, EPermission::Write);
    }

    auto factory = CreateCypressFactory(preserveAccount);

    auto* clonedImpl = factory->CloneNode(
        sourceImpl,
        removeSource ? ENodeCloneMode::Move : ENodeCloneMode::Copy);
    auto* clonedTrunkImpl = clonedImpl->GetTrunkNode();
    auto clonedProxy = GetProxy(clonedTrunkImpl);

    SetChildNode(factory, targetPath, clonedProxy, recursive);

    factory->Commit();

    if (removeSource) {
        sourceParent->RemoveChild(sourceProxy);
    }

    ToProto(response->mutable_node_id(), clonedTrunkImpl->GetId());

    context->SetRequestInfo("NodeId: %v", clonedTrunkImpl->GetId());

    context->Reply();
}

TAccessControlDescriptor* TNontemplateCypressNodeProxyBase::FindThisAcd()
{
    auto securityManager = Bootstrap_->GetSecurityManager();
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

TIntrusivePtr<const ICompositeNode> TNontemplateCompositeCypressNodeProxyBase::AsComposite() const
{
    return this;
}

TIntrusivePtr<ICompositeNode> TNontemplateCompositeCypressNodeProxyBase::AsComposite()
{
    return this;
}

void TNontemplateCompositeCypressNodeProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    descriptors->push_back("count");
    TNontemplateCypressNodeProxyBase::ListSystemAttributes(descriptors);
}

bool TNontemplateCompositeCypressNodeProxyBase::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "count") {
        BuildYsonFluently(consumer)
            .Value(GetChildCount());
        return true;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

bool TNontemplateCompositeCypressNodeProxyBase::CanHaveChildren() const
{
    return true;
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
    auto keyToChild = GetMapNodeChildren(Bootstrap_, TrunkNode, Transaction);

    // Take shared locks for children.
    typedef std::pair<Stroka, TCypressNodeBase*> TChild;
    std::vector<TChild> children;
    children.reserve(keyToChild.size());
    for (const auto& pair : keyToChild) {
        LockThisImpl(TLockRequest::SharedChild(pair.first));
        auto* childImpl = LockImpl(pair.second);
        children.push_back(std::make_pair(pair.first, childImpl));
    }

    // Insert tombstones (if in transaction).
    for (const auto& pair : children) {
        const auto& key = pair.first;
        auto* child = pair.second;
        DoRemoveChild(impl, key, child);
    }

    SetModified();
}

int TMapNodeProxy::GetChildCount() const
{
    auto cypressManager = Bootstrap_->GetCypressManager();
    auto originators = cypressManager->GetNodeOriginators(Transaction, TrunkNode);

    int result = 0;
    for (const auto* node : originators) {
        const auto* mapNode = static_cast<const TMapNode*>(node);
        result += mapNode->ChildCountDelta();
    }
    return result;
}

std::vector< std::pair<Stroka, INodePtr> > TMapNodeProxy::GetChildren() const
{
    auto keyToChild = GetMapNodeChildren(Bootstrap_, TrunkNode, Transaction);

    std::vector< std::pair<Stroka, INodePtr> > result;
    result.reserve(keyToChild.size());
    for (const auto& pair : keyToChild) {
        result.push_back(std::make_pair(pair.first, GetProxy(pair.second)));
    }

    return result;
}

std::vector<Stroka> TMapNodeProxy::GetKeys() const
{
    auto keyToChild = GetMapNodeChildren(Bootstrap_, TrunkNode, Transaction);

    std::vector<Stroka> result;
    for (const auto& pair : keyToChild) {
        result.push_back(pair.first);
    }

    return result;
}

INodePtr TMapNodeProxy::FindChild(const Stroka& key) const
{
    auto* childTrunkNode = FindMapNodeChild(Bootstrap_, TrunkNode, Transaction, key);
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

    AttachChild(Bootstrap_, TrunkNode, childImpl);

    SetModified();

    return true;
}

bool TMapNodeProxy::RemoveChild(const Stroka& key)
{
    auto* trunkChildImpl = FindMapNodeChild(Bootstrap_, TrunkNode, Transaction, key);
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
    DetachChild(Bootstrap_, TrunkNode, oldChildImpl, ownsOldChild);

    keyToChild[key] = newTrunkChildImpl;
    YCHECK(childToKey.insert(std::make_pair(newTrunkChildImpl, key)).second);
    AttachChild(Bootstrap_, TrunkNode, newChildImpl);

    SetModified();
}

Stroka TMapNodeProxy::GetChildKey(IConstNodePtr child)
{
    auto* trunkChildImpl = ToProxy(child)->GetTrunkNode();

    auto cypressManager = Bootstrap_->GetCypressManager();
    auto originators = cypressManager->GetNodeOriginators(Transaction, TrunkNode);

    for (const auto* node : originators) {
        const auto* mapNode = static_cast<const TMapNode*>(node);
        auto it = mapNode->ChildToKey().find(trunkChildImpl);
        if (it != mapNode->ChildToKey().end()) {
            return it->second;
        }
    }

    // COMPAT(babenko)
    return "?";
}

bool TMapNodeProxy::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    return TBase::DoInvoke(context);
}

void TMapNodeProxy::SetChildNode(
    INodeFactoryPtr factory,
    const TYPath& path,
    INodePtr value,
    bool recursive)
{
    TMapNodeMixin::SetChild(factory, path, value, recursive, Config->MaxNodeChildCount);
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
            YCHECK(keyToChild.insert(std::make_pair(key, nullptr)).second);
            DetachChild(Bootstrap_, TrunkNode, childImpl, false);
        } else {
            it->second = nullptr;
            YCHECK(childToKey.erase(trunkChildImpl) == 1);
            DetachChild(Bootstrap_, TrunkNode, childImpl, true);
        }
    } else {
        YCHECK(keyToChild.erase(key) == 1);
        YCHECK(childToKey.erase(trunkChildImpl) == 1);
        DetachChild(Bootstrap_, TrunkNode, childImpl, true);
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
    for (auto* trunkChild : impl->IndexToChild()) {
        children.push_back(LockImpl(trunkChild));
    }

    // Detach children.
    for (auto* child : children) {
        DetachChild(Bootstrap_, TrunkNode, child, true);
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
    for (auto* child : indexToChild) {
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
        YCHECK(impl->ChildToIndex().insert(std::make_pair(trunkChildImpl, static_cast<int>(list.size()))).second);
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

    AttachChild(Bootstrap_, TrunkNode, childImpl);

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
    DetachChild(Bootstrap_, TrunkNode, childImpl, true);

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

    DetachChild(Bootstrap_, TrunkNode, oldChildImpl, true);

    impl->IndexToChild()[index] = newTrunkChildImpl;
    impl->ChildToIndex().erase(it);
    YCHECK(impl->ChildToIndex().insert(std::make_pair(newTrunkChildImpl, index)).second);
    AttachChild(Bootstrap_, TrunkNode, newChildImpl);

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

void TListNodeProxy::SetChildNode(
    INodeFactoryPtr factory,
    const TYPath& path,
    INodePtr value,
    bool recursive)
{
    TListNodeMixin::SetChild(factory, path, value, recursive, Config->MaxNodeChildCount);
}

IYPathService::TResolveResult TListNodeProxy::ResolveRecursive(
    const TYPath& path,
    IServiceContextPtr context)
{
    return TListNodeMixin::ResolveRecursive(path, context);
}

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeProxy::TDoesNotExistService
    : public TYPathServiceBase
    , public TSupportsExists
{
private:
    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Exists);
        return TYPathServiceBase::DoInvoke(context);
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        return TResolveResult::Here(path);
    }

    virtual void ExistsSelf(
        TReqExists* /*request*/,
        TRspExists* /*response*/,
        TCtxExistsPtr context) override
    {
        ExistsAny(context);
    }

    virtual void ExistsRecursive(
        const TYPath& /*path*/,
        TReqExists* /*request*/,
        TRspExists* /*response*/,
        TCtxExistsPtr context) override
    {
        ExistsAny(context);
    }

    virtual void ExistsAttribute(
        const TYPath& /*path*/,
        TReqExists* /*request*/,
        TRspExists* /*response*/,
        TCtxExistsPtr context) override
    {
        ExistsAny(context);
    }

    void ExistsAny(TCtxExistsPtr context)
    {
        context->SetRequestInfo();
        Reply(context, false);
    }
};

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
    const auto& method = context->GetMethod();

    auto propagate = [&] () -> TResolveResult {
        if (method == "Exists") {
            auto proxy = FindTargetProxy();
            static const auto doesNotExistService = New<TDoesNotExistService>();
            return proxy
                ? TResolveResult::There(proxy, path)
                : TResolveResult::There(doesNotExistService, path);
        } else {
            return TResolveResult::There(GetTargetProxy(), path);
        }
    };

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Ampersand:
            return TBase::Resolve(tokenizer.GetSuffix(), context);

        case NYPath::ETokenType::EndOfStream: {
            // NB: Always handle Remove and Create locally.
            if (method == "Remove" || method == "Create") {
                return TResolveResult::Here(path);
            } else if (method == "Exists") {
                return propagate();
            } else {
                return TResolveResult::There(GetTargetProxy(), path);
            }
        }

        default:
            return propagate();
    }
}

void TLinkNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    descriptors->push_back(TAttributeDescriptor("target_id")
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("target_path")
        .SetOpaque(true));
    descriptors->push_back("broken");
}

bool TLinkNodeProxy::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    const auto* impl = GetThisTypedImpl();
    const auto& targetId = impl->GetTargetId();

    if (key == "target_id") {
        BuildYsonFluently(consumer)
            .Value(targetId);
        return true;
    }

    if (key == "target_path") {
        auto target = FindTargetProxy();
        if (target) {
            auto objectManager = Bootstrap_->GetObjectManager();
            auto* resolver = objectManager->GetObjectResolver();
            auto path = resolver->GetPath(target);
            BuildYsonFluently(consumer)
                .Value(path);
        } else {
            BuildYsonFluently(consumer)
                .Value(FromObjectId(impl->GetTargetId()));
        }
        return true;
    }

    if (key == "broken") {
        BuildYsonFluently(consumer)
            .Value(IsBroken(targetId));
        return true;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TLinkNodeProxy::SetBuiltinAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "target_id") {
        auto targetId = ConvertTo<TObjectId>(value);
        auto* impl = LockThisTypedImpl();
        impl->SetTargetId(targetId);
        return true;
    }

    if (key == "target_path") {
        auto targetPath = ConvertTo<Stroka>(value);
        auto objectManager = Bootstrap_->GetObjectManager();
        auto* resolver = objectManager->GetObjectResolver();
        auto targetProxy = resolver->ResolvePath(targetPath, Transaction);
        auto* impl = LockThisTypedImpl();
        impl->SetTargetId(targetProxy->GetId());
        return true;
    }

    return TBase::SetBuiltinAttribute(key, value);
}

IObjectProxyPtr TLinkNodeProxy::FindTargetProxy() const
{
    const auto* impl = GetThisTypedImpl();
    const auto& targetId = impl->GetTargetId();

    if (IsBroken(targetId)) {
        return nullptr;
    }

    auto objectManager = Bootstrap_->GetObjectManager();
    auto* target = objectManager->GetObject(targetId);
    return objectManager->GetProxy(target, Transaction);
}

IObjectProxyPtr TLinkNodeProxy::GetTargetProxy() const
{
    auto result = FindTargetProxy();
    if (!result) {
        const auto* impl = GetThisTypedImpl();
        THROW_ERROR_EXCEPTION("Link target %v does not exist",
            impl->GetTargetId());
    }
    return result;
}

bool TLinkNodeProxy::IsBroken(const NObjectServer::TObjectId& id) const
{
    if (IsVersionedType(TypeFromId(id))) {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedNodeId(id));
        if (!node) {
            return true;
        }
        if (!cypressManager->IsAlive(node, Transaction)) {
            return true;
        }
        return false;
    } else {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto* obj = objectManager->FindObject(id);
        return !IsObjectAlive(obj);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDocumentNodeProxy::TDocumentNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TDocumentNode* trunkNode)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
{ }

ENodeType TDocumentNodeProxy::GetType() const 
{
    return ENodeType::Entity;
}

TIntrusivePtr<const IEntityNode> TDocumentNodeProxy::AsEntity() const
{
    return this;
}

TIntrusivePtr<IEntityNode> TDocumentNodeProxy::AsEntity()
{
    return this;
}

IYPathService::TResolveResult TDocumentNodeProxy::ResolveRecursive(const TYPath& path, IServiceContextPtr context)
{
    return TResolveResult::Here("/" + path);
}

namespace {

template <class TServerRequest, class TServerResponse, class TContext>
void DelegateInvocation(
    IYPathServicePtr service,
    TServerRequest* serverRequest,
    TServerResponse* serverResponse,
    TIntrusivePtr<TContext> context)
{
    typedef typename TServerRequest::TMessage  TRequestMessage;
    typedef typename TServerResponse::TMessage TResponseMessage;
    
    typedef TTypedYPathRequest<TRequestMessage, TResponseMessage>  TClientRequest;
    typedef TTypedYPathResponse<TRequestMessage, TResponseMessage> TClientResponse;

    auto clientRequest = New<TClientRequest>(context->RequestHeader());
    clientRequest->MergeFrom(*serverRequest);

    auto clientResponseOrError = ExecuteVerb(service, clientRequest).Get();

    if (clientResponseOrError.IsOK()) {
        const auto& clientResponse = clientResponseOrError.Value();
        serverResponse->MergeFrom(*clientResponse);
        context->Reply();
    } else {
        context->Reply(clientResponseOrError);
    }
}

} // namespace

void TDocumentNodeProxy::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    const auto* impl = GetThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::GetRecursive(const TYPath& /*path*/, TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    const auto* impl = GetThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::SetSelf(TReqSet* request, TRspSet* /*response*/, TCtxSetPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
    auto* impl = LockThisTypedImpl();
    impl->SetValue(ConvertToNode(TYsonString(request->value())));
    context->Reply();
}

void TDocumentNodeProxy::SetRecursive(const TYPath& /*path*/, TReqSet* request, TRspSet* response, TCtxSetPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
    auto* impl = LockThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::ListSelf(TReqList* request, TRspList* response, TCtxListPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    const auto* impl = GetThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::ListRecursive(const TYPath& /*path*/, TReqList* request, TRspList* response, TCtxListPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    const auto* impl = GetThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::RemoveRecursive(const TYPath& /*path*/, TReqRemove* request, TRspRemove* response, TCtxRemovePtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
    auto* impl = LockThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::ExistsRecursive(const TYPath& /*path*/, TReqExists* request, TRspExists* response, TCtxExistsPtr context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    const auto* impl = GetThisTypedImpl();
    DelegateInvocation(impl->GetValue(), request, response, context);
}

void TDocumentNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    descriptors->push_back(TAttributeDescriptor("value")
        .SetOpaque(true)
        .SetReplicated(true));
}

bool TDocumentNodeProxy::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    const auto* impl = GetThisTypedImpl();

    if (key == "value") {
        BuildYsonFluently(consumer)
            .Value(impl->GetValue());
        return true;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TDocumentNodeProxy::SetBuiltinAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "value") {
        auto* impl = LockThisTypedImpl();
        impl->SetValue(ConvertToNode(value));
        return true;
    }

    return TBase::SetBuiltinAttribute(key, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

