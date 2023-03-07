#include "node_proxy_detail.h"
#include "private.h"
#include "cypress_traverser.h"
#include "helpers.h"
#include "shard.h"

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config_manager.h>

#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/chunk_owner_base.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/security_server/access_log.h>
#include <yt/server/master/security_server/account.h>
#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/master/tablet_server/tablet_cell_bundle.h>
#include <yt/server/master/tablet_server/tablet_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/logging/fluent_log.h>

#include <yt/core/misc/string.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/exception_helpers.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node_detail.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_detail.h>

#include <yt/core/yson/async_writer.h>

#include <yt/core/compression//codec.h>

#include <type_traits>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NLogging;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsAccessLoggedMethod(const TString& method) {
    static const THashSet<TString> methodsForAccessLog = {
        "Lock",
        "Unlock",
        "GetKey",
        "Get",
        "Set",
        "Remove",
        "List",
        "Exists",
        "GetBasicAttributes",
        "CheckPermission",
        "BeginCopy",
        "EndCopy"
    };
    return methodsForAccessLog.contains(method);
}

bool HasTrivialAcd(const TCypressNode* node)
{
    const auto& acd = node->Acd();
    return acd.GetInherit() && acd.Acl().Entries.empty();
}

bool CheckItemReadPermissions(
    TCypressNode* parent,
    TCypressNode* child,
    const TSecurityManagerPtr& securityManager)
{
    // Fast path.
    if ((!parent || HasTrivialAcd(parent)) && HasTrivialAcd(child)) {
        return true;
    }

    // Slow path.
    auto* user = securityManager->GetAuthenticatedUser();
    return securityManager->CheckPermission(child, user, EPermission::Read).Action == ESecurityAction::Allow;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::TCustomAttributeDictionary(
    TNontemplateCypressNodeProxyBase* proxy)
    : Proxy_(proxy)
{ }

std::vector<TString> TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::ListKeys() const
{
    auto keys = ListNodeAttributes(
        Proxy_->Bootstrap_->GetCypressManager(),
        Proxy_->TrunkNode_,
        Proxy_->Transaction_);
    return std::vector<TString>(keys.begin(), keys.end());
}

std::vector<IAttributeDictionary::TKeyValuePair> TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::ListPairs() const
{
    auto pairs = GetNodeAttributes(
        Proxy_->Bootstrap_->GetCypressManager(),
        Proxy_->TrunkNode_,
        Proxy_->Transaction_);
    return std::vector<TKeyValuePair>(pairs.begin(), pairs.end());
}

TYsonString TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::FindYson(TStringBuf name) const
{
    const auto& cypressManager = Proxy_->Bootstrap_->GetCypressManager();
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

    return {};
}

void TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::SetYson(const TString& key, const TYsonString& value)
{
    YT_ASSERT(value);

    auto oldValue = FindYson(key);
    Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, value);

    const auto& cypressManager = Proxy_->Bootstrap_->GetCypressManager();
    auto* node = cypressManager->LockNode(
        Proxy_->TrunkNode_,
        Proxy_->Transaction_,
        TLockRequest::MakeSharedAttribute(key));

    const auto& securityManager = Proxy_->Bootstrap_->GetSecurityManager();
    const auto& multicellManager = Proxy_->Bootstrap_->GetMulticellManager();
    if (Proxy_->TrunkNode_->GetNativeCellTag() == multicellManager->GetCellTag()) {
        auto resourceUsageIncrease = TClusterResources()
            .SetMasterMemory(1);
        securityManager->ValidateResourceUsageIncrease(node->GetAccount(), resourceUsageIncrease);
    }

    auto* userAttributes = node->GetMutableAttributes();
    userAttributes->Set(key, value);

    securityManager->UpdateMasterMemoryUsage(node);

    Proxy_->SetModified(EModificationType::Attributes);
}

bool TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::Remove(const TString& key)
{
    auto oldValue = FindYson(key);
    if (!oldValue) {
        return false;
    }

    Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, {});

    const auto& cypressManager = Proxy_->Bootstrap_->GetCypressManager();
    auto* node = cypressManager->LockNode(
        Proxy_->TrunkNode_,
        Proxy_->Transaction_,
        TLockRequest::MakeSharedAttribute(key));

    auto* userAttributes = node->GetMutableAttributes();
    if (node->GetTransaction()) {
        userAttributes->Set(key, {});
    } else {
        YT_VERIFY(userAttributes->Remove(key));
    }

    const auto& securityManager = Proxy_->Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(node);

    Proxy_->SetModified(EModificationType::Attributes);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeProxyBase::TResourceUsageVisitor
    : public ICypressNodeVisitor
{
public:
    TResourceUsageVisitor(
        NCellMaster::TBootstrap* bootstrap,
        ICypressNodeProxyPtr rootNode)
        : Bootstrap_(bootstrap)
        , RootNode_(std::move(rootNode))
    { }

    TPromise<TYsonString> Run()
    {
        TraverseCypress(
            Bootstrap_->GetCypressManager(),
            Bootstrap_->GetTransactionManager(),
            Bootstrap_->GetObjectManager(),
            Bootstrap_->GetSecurityManager(),
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::CypressTraverser),
            RootNode_->GetTrunkNode(),
            RootNode_->GetTransaction(),
            this);
        return Promise_;
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const ICypressNodeProxyPtr RootNode_;

    TPromise<TYsonString> Promise_ = NewPromise<TYsonString>();
    TClusterResources ResourceUsage_;


    virtual void OnNode(TCypressNode* trunkNode, TTransaction* transaction) override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->GetVersionedNode(trunkNode, transaction);
        ResourceUsage_ += node->GetTotalResourceUsage();
    }

    virtual void OnError(const TError& error) override
    {
        auto wrappedError = TError("Error computing recursive resource usage")
            << error;
        Promise_.Set(wrappedError);
    }

    virtual void OnCompleted() override
    {
        auto usage = New<TSerializableClusterResources>(Bootstrap_->GetChunkManager(), ResourceUsage_);
        Promise_.Set(ConvertToYsonString(usage));
    }
};

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TCypressNode* trunkNode)
    : TObjectProxyBase(bootstrap, metadata, trunkNode)
    , THierarchicPermissionValidator(CreatePermissionValidator())
    , CustomAttributesImpl_(this)
    , Transaction_(transaction)
    , TrunkNode_(trunkNode)
{
    YT_ASSERT(TrunkNode_);
    YT_ASSERT(TrunkNode_->IsTrunk());

    CustomAttributes_ = &CustomAttributesImpl_;
}

std::unique_ptr<ITransactionalNodeFactory> TNontemplateCypressNodeProxyBase::CreateFactory() const
{
    auto* account = GetThisImpl()->GetAccount();
    return CreateCypressFactory(account, TNodeFactoryOptions());
}

std::unique_ptr<ICypressNodeFactory> TNontemplateCypressNodeProxyBase::CreateCypressFactory(
    TAccount* account,
    const TNodeFactoryOptions& options) const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->CreateNodeFactory(
        GetThisImpl()->GetTrunkNode()->GetShard(),
        Transaction_,
        account,
        options);
}

TYPath TNontemplateCypressNodeProxyBase::GetPath() const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->GetNodePath(this);
}

TTransaction* TNontemplateCypressNodeProxyBase::GetTransaction() const
{
    return Transaction_;
}

TCypressNode* TNontemplateCypressNodeProxyBase::GetTrunkNode() const
{
    return TrunkNode_;
}

ICompositeNodePtr TNontemplateCypressNodeProxyBase::GetParent() const
{
    auto* parent = GetThisImpl()->GetParent();
    return parent ? GetProxy(parent)->AsComposite() : nullptr;
}

void TNontemplateCypressNodeProxyBase::SetParent(const ICompositeNodePtr& parent)
{
    auto* impl = LockThisImpl();
    impl->SetParent(parent ? ICypressNodeProxy::FromNode(parent.Get())->GetTrunkNode() : nullptr);
}

const IAttributeDictionary& TNontemplateCypressNodeProxyBase::Attributes() const
{
    return TObjectProxyBase::Attributes();
}

IAttributeDictionary* TNontemplateCypressNodeProxyBase::MutableAttributes()
{
    return TObjectProxyBase::MutableAttributes();
}

TFuture<TYsonString> TNontemplateCypressNodeProxyBase::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::RecursiveResourceUsage: {
            auto visitor = New<TResourceUsageVisitor>(Bootstrap_, this);
            return visitor->Run();
        }

        default:
            break;
    }

    auto asyncResult = GetExternalBuiltinAttributeAsync(key);
    if (asyncResult) {
        return asyncResult;
    }

    return TObjectProxyBase::GetBuiltinAttributeAsync(key);
}

TFuture<TYsonString> TNontemplateCypressNodeProxyBase::GetExternalBuiltinAttributeAsync(TInternedAttributeKey internedKey)
{
    const auto* node = GetThisImpl();
    if (!node->IsExternal()) {
        return std::nullopt;
    }

    auto optionalDescriptor = FindBuiltinAttributeDescriptor(internedKey);
    if (!optionalDescriptor) {
        return std::nullopt;
    }

    const auto& descriptor = *optionalDescriptor;
    if (!descriptor.External || !descriptor.Present) {
        return std::nullopt;
    }

    auto externalCellTag = node->GetExternalCellTag();

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    auto transactionId = transactionManager->GetNearestExternalizedTransactionAncestor(
        GetTransaction(),
        externalCellTag);

    auto key = internedKey.Unintern();

    auto req = TYPathProxy::Get(FromObjectId(GetId()) + "/@" + key);
    AddCellTagToSyncWith(req, GetId());
    SetTransactionId(req, transactionId);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto channel = multicellManager->GetMasterChannelOrThrow(
        externalCellTag,
        NHydra::EPeerKind::Follower);

    TObjectServiceProxy proxy(channel);
    return proxy.Execute(req).Apply(BIND([=, this_ = MakeStrong(this)] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) {
        if (!rspOrError.IsOK()) {
            auto code = rspOrError.GetCode();
            if (code == NYTree::EErrorCode::ResolveError || code == NTransactionClient::EErrorCode::NoSuchTransaction) {
                return TYsonString();
            }
            THROW_ERROR_EXCEPTION("Error requesting attribute %Qv of object %v from cell %v",
                key,
                GetVersionedId(),
                externalCellTag)
                << rspOrError;
        }

        const auto& rsp = rspOrError.Value();
        return TYsonString(rsp->value());
    }));
}

bool TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value)
{
    switch (key) {
        case EInternedAttributeKey::Account: {
            ValidateNoTransaction();

            const auto& securityManager = Bootstrap_->GetSecurityManager();

            auto name = ConvertTo<TString>(value);
            auto* account = securityManager->GetAccountByNameOrThrow(name);
            account->ValidateActiveLifeStage();

            ValidateStorageParametersUpdate();
            ValidatePermission(account, EPermission::Use);

            auto* node = LockThisImpl();
            if (node->GetAccount() != account) {
                // TODO(savrus) See YT-7050
                securityManager->ValidateResourceUsageIncrease(account, TClusterResources().SetNodeCount(1));
                securityManager->SetAccount(node, account, /* transaction */ nullptr);
            }

            return true;
        }

        case EInternedAttributeKey::ExpirationTime: {
            ValidatePermission(EPermissionCheckScope::This|EPermissionCheckScope::Descendants, EPermission::Remove);

            const auto& cypressManager = Bootstrap_->GetCypressManager();

            if (TrunkNode_ == cypressManager->GetRootNode()) {
                THROW_ERROR_EXCEPTION("Cannot set \"expiration_time\" for the root");
            }

            auto time = ConvertTo<TInstant>(value);

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);

            cypressManager->SetExpirationTime(node, time);

            return true;
        }

        case EInternedAttributeKey::Opaque: {
            ValidateNoTransaction();
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            // NB: No locking, intentionally.
            auto* node = GetThisImpl();
            auto opaque = ConvertTo<bool>(value);
            node->SetOpaque(opaque);

            return true;
        }

        case EInternedAttributeKey::InheritAcl:
        case EInternedAttributeKey::Acl:
        case EInternedAttributeKey::Owner: {
            auto attributeUpdated = TObjectProxyBase::SetBuiltinAttribute(key, value);
            auto* node = GetThisImpl();
            if (attributeUpdated && !node->IsBeingCreated()) {
                LogStructuredEventFluently(CypressServerLogger, ELogLevel::Info)
                    .Item("event").Value(EAccessControlEvent::ObjectAcdUpdated)
                    .Item("attribute").Value(key.Unintern())
                    .Item("path").Value(GetPath())
                    .Item("value").Value(value);
            }
            return attributeUpdated;
        }

        case EInternedAttributeKey::Annotation: {
            auto annotation = ConvertTo<std::optional<TString>>(value);
            if (annotation) {
                ValidateAnnotation(*annotation);
            }
            auto* lockedNode = LockThisImpl();
            lockedNode->SetAnnotation(annotation);
            return true;
        }

        default:
            break;
    }

    return TObjectProxyBase::SetBuiltinAttribute(key, value);
}

bool TNontemplateCypressNodeProxyBase::RemoveBuiltinAttribute(TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::Annotation: {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& handler = objectManager->GetHandler(Object_);

            if (Any(handler->GetFlags() & ETypeFlags::ForbidAnnotationRemoval)) {
                THROW_ERROR_EXCEPTION("Cannot remove annotation from portal node; consider overriding it somewhere down the tree or setting it to an empty string");
            }

            auto* lockedNode = LockThisImpl();
            lockedNode->SetAnnotation(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ExpirationTime: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTime(node, std::nullopt);

            return true;
        }

        case EInternedAttributeKey::Opaque: {
            ValidateNoTransaction();
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            // NB: No locking, intentionally.
            auto* node = GetThisImpl();
            node->SetOpaque(false);

            return true;
        }

        default:
            break;
    }

    return TObjectProxyBase::RemoveBuiltinAttribute(key);
}

TVersionedObjectId TNontemplateCypressNodeProxyBase::GetVersionedId() const
{
    return TVersionedObjectId(Object_->GetId(), GetObjectId(Transaction_));
}

TAccessControlDescriptor* TNontemplateCypressNodeProxyBase::FindThisAcd()
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* node = GetThisImpl();
    return securityManager->FindAcd(node);
}

void TNontemplateCypressNodeProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TObjectProxyBase::ListSystemAttributes(descriptors);

    const auto* node = GetThisImpl();
    bool hasKey = NodeHasKey(node);
    bool isExternal = node->IsExternal();

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentId)
        .SetPresent(NodeHasParentId(node)));
    descriptors->push_back(EInternedAttributeKey::External);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExternalCellTag)
        .SetPresent(isExternal));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Locks)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::LockCount);
    descriptors->push_back(EInternedAttributeKey::LockMode);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Path)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Key)
        .SetPresent(hasKey));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTime)
        .SetPresent(node->TryGetExpirationTime().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(EInternedAttributeKey::CreationTime);
    descriptors->push_back(EInternedAttributeKey::ModificationTime);
    descriptors->push_back(EInternedAttributeKey::AccessTime);
    descriptors->push_back(EInternedAttributeKey::AccessCounter);
    descriptors->push_back(EInternedAttributeKey::Revision);
    descriptors->push_back(EInternedAttributeKey::AttributesRevision);
    descriptors->push_back(EInternedAttributeKey::ContentRevision);
    descriptors->push_back(EInternedAttributeKey::ResourceUsage);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveResourceUsage)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Account)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Opaque)
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ShardId)
        .SetPresent(node->GetTrunkNode()->GetShard() != nullptr));
    descriptors->push_back(EInternedAttributeKey::ResolveCached);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Annotation)
        .SetWritable(true)
        .SetRemovable(true)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AnnotationPath)
        .SetOpaque(true));
}

bool TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(
    TInternedAttributeKey key,
    IYsonConsumer* consumer)
{
    auto* node = GetThisImpl();
    const auto* trunkNode = node->GetTrunkNode();
    bool isExternal = node->IsExternal();

    switch (key) {
        case EInternedAttributeKey::ParentId:
            if (!NodeHasParentId(node)) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(GetNodeParentId(node));
            return true;

        case EInternedAttributeKey::External:
            BuildYsonFluently(consumer)
                .Value(isExternal);
            return true;

        case EInternedAttributeKey::ExternalCellTag:
            if (!isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(node->GetExternalCellTag());
            return true;

        case EInternedAttributeKey::Locks: {
            auto printLock = [=] (TFluentList fluent, const TLock* lock) {
                const auto& request = lock->Request();
                fluent.Item()
                    .BeginMap()
                        .Item("id").Value(lock->GetId())
                        .Item("state").Value(lock->GetState())
                        .Item("transaction_id").Value(lock->GetTransaction()->GetId())
                        .Item("mode").Value(request.Mode)
                        .DoIf(request.Key.Kind == ELockKeyKind::Child, [=] (TFluentMap fluent) {
                            fluent
                                .Item("child_key").Value(request.Key.Name);
                        })
                        .DoIf(request.Key.Kind == ELockKeyKind::Attribute, [=] (TFluentMap fluent) {
                            fluent
                                .Item("attribute_key").Value(request.Key.Name);
                        })
                    .EndMap();
            };

            BuildYsonFluently(consumer)
                .BeginList()
                    .DoFor(trunkNode->LockingState().AcquiredLocks, printLock)
                    .DoFor(trunkNode->LockingState().PendingLocks, printLock)
                .EndList();
            return true;
        }

        case EInternedAttributeKey::LockCount:
            BuildYsonFluently(consumer)
                .Value(trunkNode->LockingState().AcquiredLocks.size() + trunkNode->LockingState().PendingLocks.size());
            return true;

        case EInternedAttributeKey::LockMode:
            BuildYsonFluently(consumer)
                .Value(node->GetLockMode());
            return true;

        case EInternedAttributeKey::Path:
            BuildYsonFluently(consumer)
                .Value(GetPath());
            return true;

        case EInternedAttributeKey::Key: {
            static const TString NullKey("?");
            auto optionalKey = FindNodeKey(
                Bootstrap_->GetCypressManager(),
                GetThisImpl()->GetTrunkNode(),
                Transaction_);
            BuildYsonFluently(consumer)
                .Value(optionalKey.value_or(NullKey));
            return true;
        }

        case EInternedAttributeKey::ExpirationTime: {
            auto optionalExpirationTime = node->TryGetExpirationTime();
            if (!optionalExpirationTime) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*optionalExpirationTime);
            return true;
        }

        case EInternedAttributeKey::CreationTime:
            BuildYsonFluently(consumer)
                .Value(node->GetCreationTime());
            return true;

        case EInternedAttributeKey::ModificationTime:
            BuildYsonFluently(consumer)
                .Value(node->GetModificationTime());
            return true;

        case EInternedAttributeKey::AccessTime:
            BuildYsonFluently(consumer)
                .Value(trunkNode->GetAccessTime());
            return true;

        case EInternedAttributeKey::AccessCounter:
            BuildYsonFluently(consumer)
                .Value(trunkNode->GetAccessCounter());
            return true;

        case EInternedAttributeKey::Revision:
            BuildYsonFluently(consumer)
                .Value(node->GetRevision());
            return true;

        case EInternedAttributeKey::AttributesRevision:
            BuildYsonFluently(consumer)
                .Value(node->GetAttributesRevision());
            return true;

        case EInternedAttributeKey::ContentRevision:
            BuildYsonFluently(consumer)
                .Value(node->GetContentRevision());
            return true;

        case EInternedAttributeKey::ResourceUsage: {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto resourceSerializer = New<TSerializableClusterResources>(chunkManager, node->GetTotalResourceUsage());
            BuildYsonFluently(consumer)
                .Value(resourceSerializer);
            return true;
        }

        case EInternedAttributeKey::Account:
            BuildYsonFluently(consumer)
                .Value(node->GetAccount()->GetName());
            return true;

        case EInternedAttributeKey::Opaque:
            BuildYsonFluently(consumer)
                .Value(node->GetOpaque());
            return true;

        case EInternedAttributeKey::ShardId:
            if (node->GetTrunkNode()->GetShard()) {
                BuildYsonFluently(consumer)
                    .Value(node->GetTrunkNode()->GetShard()->GetId());
                return true;
            }
            break;

        case EInternedAttributeKey::ResolveCached:
            BuildYsonFluently(consumer)
                .Value(node->GetTrunkNode()->GetResolveCacheNode().operator bool());
            return true;

        case EInternedAttributeKey::Annotation: {
            const auto* annotationNode = FindClosestAncestorWithAnnotation(node);
            if (annotationNode) {
                BuildYsonFluently(consumer)
                    .Value(*annotationNode->GetAnnotation());
            } else {
                BuildYsonFluently(consumer)
                    .Entity();
            }
            return true;
        }

        case EInternedAttributeKey::AnnotationPath: {
            auto* annotationNode = FindClosestAncestorWithAnnotation(node);
            if (annotationNode) {
                const auto& cypressManager = Bootstrap_->GetCypressManager();
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(annotationNode, GetTransaction()));
            } else {
                BuildYsonFluently(consumer)
                    .Entity();
            }
            return true;
        }

        default:
            break;
    }

    return TObjectProxyBase::GetBuiltinAttribute(key, consumer);
}

void TNontemplateCypressNodeProxyBase::ValidateStorageParametersUpdate()
{ }

void TNontemplateCypressNodeProxyBase::ValidateLockPossible()
{ }

void TNontemplateCypressNodeProxyBase::BeforeInvoke(const IServiceContextPtr& context)
{
    AccessTrackingSuppressed_ = GetSuppressAccessTracking(context->RequestHeader());
    ModificationTrackingSuppressed_ = GetSuppressModificationTracking(context->RequestHeader());

    TObjectProxyBase::BeforeInvoke(context);
}

void TNontemplateCypressNodeProxyBase::AfterInvoke(const IServiceContextPtr& context)
{
    SetAccessed();
    TObjectProxyBase::AfterInvoke(context);
}

bool TNontemplateCypressNodeProxyBase::DoInvoke(const NRpc::IServiceContextPtr& context)
{
    ValidateAccessTransaction();

    YT_LOG_ACCESS_IF(
        IsAccessLoggedMethod(context->GetMethod()),
        context,
        GetPath(),
        Transaction_);

    DISPATCH_YPATH_SERVICE_METHOD(Lock);
    DISPATCH_YPATH_SERVICE_METHOD(Create);
    DISPATCH_YPATH_SERVICE_METHOD(Copy);
    DISPATCH_YPATH_SERVICE_METHOD(BeginCopy);
    DISPATCH_YPATH_SERVICE_METHOD(EndCopy);
    DISPATCH_YPATH_SERVICE_METHOD(Unlock);

    if (TNodeBase::DoInvoke(context)) {
        return true;
    }

    if (TObjectProxyBase::DoInvoke(context)) {
        return true;
    }

    return false;
}

void TNontemplateCypressNodeProxyBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    class TVisitor
    {
    public:
        TVisitor(
            TCypressManagerPtr cypressManager,
            TSecurityManagerPtr securityManager,
            TTransaction* transaction,
            std::optional<std::vector<TString>> attributeKeys)
            : CypressManager_(std::move(cypressManager))
            , SecurityManager_(std::move(securityManager))
            , Transaction_(transaction)
            , AttributeKeys_(std::move(attributeKeys))
        { }

        void Run(TCypressNode* root)
        {
            VisitAny(nullptr, root);
        }

        TFuture<TYsonString> Finish()
        {
            return Writer_.Finish();
        }

    private:
        const TCypressManagerPtr CypressManager_;
        const TSecurityManagerPtr SecurityManager_;
        TTransaction* const Transaction_;
        const std::optional<std::vector<TString>> AttributeKeys_;

        TAsyncYsonWriter Writer_;


        void VisitAny(TCypressNode* trunkParent, TCypressNode* trunkChild)
        {
            if (!CheckItemReadPermissions(trunkParent, trunkChild, SecurityManager_)) {
                Writer_.OnEntity();
                return;
            }

            auto proxy = CypressManager_->GetNodeProxy(trunkChild, Transaction_);
            proxy->WriteAttributes(&Writer_, AttributeKeys_, false);

            if (trunkParent && trunkChild->GetOpaque()) {
                Writer_.OnEntity();
                return;
            }

            switch (trunkChild->GetNodeType()) {
                case ENodeType::List:
                    VisitList(trunkChild->As<TListNode>());
                    break;
                case ENodeType::Map:
                    VisitMap(trunkChild->As<TMapNode>());
                    break;
                default:
                    VisitOther(trunkChild);
                    break;
            }
        }

        void VisitOther(TCypressNode* trunkNode)
        {
            auto* node = CypressManager_->GetVersionedNode(trunkNode, Transaction_);
            switch (node->GetType()) {
                case EObjectType::StringNode:
                    Writer_.OnStringScalar(node->As<TStringNode>()->Value());
                    break;
                case EObjectType::Int64Node:
                    Writer_.OnInt64Scalar(node->As<TInt64Node>()->Value());
                    break;
                case EObjectType::Uint64Node:
                    Writer_.OnUint64Scalar(node->As<TUint64Node>()->Value());
                    break;
                case EObjectType::DoubleNode:
                    Writer_.OnDoubleScalar(node->As<TDoubleNode>()->Value());
                    break;
                case EObjectType::BooleanNode:
                    Writer_.OnBooleanScalar(node->As<TBooleanNode>()->Value());
                    break;
                default:
                    Writer_.OnEntity();
                    break;
            }
        }

        void VisitList(TCypressNode* node)
        {
            Writer_.OnBeginList();
            const auto& childList = GetListNodeChildList(
                CypressManager_,
                node->As<TListNode>(),
                Transaction_);
            for (auto* child : childList) {
                Writer_.OnListItem();
                VisitAny(node, child);
            }
            Writer_.OnEndList();
        }

        void VisitMap(TCypressNode* node)
        {
            Writer_.OnBeginMap();
            THashMap<TString, TCypressNode*> keyToChildMapStorage;
            const auto& keyToChildMap = GetMapNodeChildMap(
                CypressManager_,
                node->As<TMapNode>(),
                Transaction_,
                &keyToChildMapStorage);
            for (const auto& pair : keyToChildMap) {
                Writer_.OnKeyedItem(pair.first);
                VisitAny(node, pair.second);
            }
            Writer_.OnEndMap();
        }
    };

    auto attributeKeys = request->has_attributes()
        ? std::make_optional(FromProto<std::vector<TString>>(request->attributes().keys()))
        : std::nullopt;

    // TODO(babenko): make use of limit
    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("AttributeKeys: %v, Limit: %v",
        attributeKeys,
        limit);

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    TVisitor visitor(
        Bootstrap_->GetCypressManager(),
        Bootstrap_->GetSecurityManager(),
        Transaction_,
        std::move(attributeKeys));
    visitor.Run(TrunkNode_);
    visitor.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().GetData());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TNontemplateCypressNodeProxyBase::DoRemoveSelf()
{
    auto* node = GetThisImpl();
    if (node->GetType() == EObjectType::PortalExit) {
        // XXX(babenko)
        if (Transaction_) {
            THROW_ERROR_EXCEPTION("Removing portal in transaction is not supported");
        }
        YT_VERIFY(node->IsTrunk());

        LockImpl(node, ELockMode::Exclusive, true);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(node);
    } else {
        TNodeBase::DoRemoveSelf();
    }
}

void TNontemplateCypressNodeProxyBase::GetAttribute(
    const TYPath& path,
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    SuppressAccessTracking();
    TObjectProxyBase::GetAttribute(path, request, response, context);
}

void TNontemplateCypressNodeProxyBase::ListAttribute(
    const TYPath& path,
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ListAttribute(path, request, response, context);
}

void TNontemplateCypressNodeProxyBase::ExistsSelf(
    TReqExists* request,
    TRspExists* response,
    const TCtxExistsPtr& context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ExistsSelf(request, response, context);
}

void TNontemplateCypressNodeProxyBase::ExistsRecursive(
    const TYPath& path,
    TReqExists* request,
    TRspExists* response,
    const TCtxExistsPtr& context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ExistsRecursive(path, request, response, context);
}

void TNontemplateCypressNodeProxyBase::ExistsAttribute(
    const TYPath& path,
    TReqExists* request,
    TRspExists* response,
    const TCtxExistsPtr& context)
{
    SuppressAccessTracking();
    TObjectProxyBase::ExistsAttribute(path, request, response, context);
}

TCypressNode* TNontemplateCypressNodeProxyBase::GetImpl(TCypressNode* trunkNode) const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->GetVersionedNode(trunkNode, Transaction_);
}

TCypressNode* TNontemplateCypressNodeProxyBase::LockImpl(
    TCypressNode* trunkNode,
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/) const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->LockNode(trunkNode, Transaction_, request, recursive);
}

TCypressNode* TNontemplateCypressNodeProxyBase::DoGetThisImpl()
{
    if (CachedNode_) {
        return CachedNode_;
    }
    auto* node = GetImpl(TrunkNode_);
    if (node->GetTransaction() == Transaction_) {
        CachedNode_ = node;
    }
    return node;
}

TCypressNode* TNontemplateCypressNodeProxyBase::DoLockThisImpl(
    const TLockRequest& request /*= ELockMode::Exclusive*/,
    bool recursive /*= false*/)
{
    CachedNode_ = LockImpl(TrunkNode_, request, recursive);
    YT_ASSERT(CachedNode_->GetTransaction() == Transaction_);
    return CachedNode_;
}

void TNontemplateCypressNodeProxyBase::GatherInheritableAttributes(TCypressNode* parent, TCompositeNodeBase::TAttributes* attributes)
{
    for (auto* ancestor = parent; ancestor && !attributes->AreFull(); ancestor = ancestor->GetParent()) {
        auto* compositeAncestor = ancestor->As<TCompositeNodeBase>();

#define XX(camelCaseName, snakeCaseName) \
        { \
            auto inheritedValue = compositeAncestor->Get##camelCaseName(); \
            if (!attributes->camelCaseName && inheritedValue) { \
                attributes->camelCaseName = inheritedValue; \
            } \
        }

        if (compositeAncestor->HasInheritableAttributes()) {
            FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
        }
#undef XX
    }
}

ICypressNodeProxyPtr TNontemplateCypressNodeProxyBase::GetProxy(TCypressNode* trunkNode) const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->GetNodeProxy(trunkNode, Transaction_);
}

void TNontemplateCypressNodeProxyBase::ValidatePermission(
    EPermissionCheckScope scope,
    EPermission permission,
    const TString& /* user */)
{
    auto* node = GetThisImpl();
    // NB: Suppress permission checks for nodes upon construction.
    // Cf. YT-1191, YT-4628.
    auto* trunkNode = node->GetTrunkNode();
    auto* shard = trunkNode->GetShard();
    if (trunkNode->GetParent() ||
        shard && trunkNode == shard->GetRoot())
    {
        ValidatePermission(node, scope, permission);
    }
}

SmallVector<TCypressNode*, 1> TNontemplateCypressNodeProxyBase::ListDescendants(TCypressNode* node)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* trunkNode = node->GetTrunkNode();
    return cypressManager->ListSubtreeNodes(trunkNode, Transaction_, false);
}

void TNontemplateCypressNodeProxyBase::ValidateNotExternal()
{
    if (TrunkNode_->IsExternal()) {
        THROW_ERROR_EXCEPTION("Operation cannot be performed at an external node");
    }
}

void TNontemplateCypressNodeProxyBase::ValidateMediaChange(
    const std::optional<TChunkReplication>& oldReplication,
    std::optional<int> primaryMediumIndex,
    const TChunkReplication& newReplication)
{
    if (newReplication == oldReplication) {
        return;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    for (const auto& entry : newReplication) {
        if (entry.Policy()) {
            auto* medium = chunkManager->GetMediumByIndex(entry.GetMediumIndex());
            ValidatePermission(medium, EPermission::Use);
        }
    }

    if (primaryMediumIndex && !newReplication.Get(*primaryMediumIndex)) {
        const auto* primaryMedium = chunkManager->GetMediumByIndex(*primaryMediumIndex);
        THROW_ERROR_EXCEPTION("Cannot remove primary medium %Qv",
            primaryMedium->GetName());
    }

    ValidateChunkReplication(chunkManager, newReplication, primaryMediumIndex);
}

bool TNontemplateCypressNodeProxyBase::ValidatePrimaryMediumChange(
    TMedium* newPrimaryMedium,
    const TChunkReplication& oldReplication,
    std::optional<int> oldPrimaryMediumIndex,
    TChunkReplication* newReplication)
{
    auto newPrimaryMediumIndex = newPrimaryMedium->GetIndex();
    if (newPrimaryMediumIndex == oldPrimaryMediumIndex) {
        return false;
    }

    ValidatePermission(newPrimaryMedium, EPermission::Use);

    auto copiedReplication = oldReplication;
    if (!copiedReplication.Get(newPrimaryMediumIndex) && oldPrimaryMediumIndex) {
        // The user is trying to set a medium with zero replication count
        // as primary. This is regarded as a request to move from one medium to
        // another.
        copiedReplication.Set(newPrimaryMediumIndex, copiedReplication.Get(*oldPrimaryMediumIndex));
        copiedReplication.Erase(*oldPrimaryMediumIndex);
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    ValidateChunkReplication(chunkManager, copiedReplication, newPrimaryMediumIndex);

    *newReplication = copiedReplication;

    return true;
}

void TNontemplateCypressNodeProxyBase::SetModified(EModificationType modificationType)
{
    if (ModificationTrackingSuppressed_) {
        return;
    }

    if (!TrunkNode_->IsAlive()) {
        return;
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    if (!CachedNode_) {
        CachedNode_ = cypressManager->GetNode(GetVersionedId());
    }

    cypressManager->SetModified(CachedNode_, modificationType);
}

void TNontemplateCypressNodeProxyBase::SuppressModificationTracking()
{
    ModificationTrackingSuppressed_ = true;
}

void TNontemplateCypressNodeProxyBase::SetAccessed()
{
    if (AccessTrackingSuppressed_) {
        return;
    }

    if (!TrunkNode_->IsAlive()) {
        return;
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    cypressManager->SetAccessed(TrunkNode_);
}

void TNontemplateCypressNodeProxyBase::SuppressAccessTracking()
{
    AccessTrackingSuppressed_ = true;
}

bool TNontemplateCypressNodeProxyBase::CanHaveChildren() const
{
    return false;
}

void TNontemplateCypressNodeProxyBase::SetChildNode(
    INodeFactory* /*factory*/,
    const TYPath& /*path*/,
    const INodePtr& /*child*/,
    bool /*recursive*/)
{
    YT_ABORT();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Lock)
{
    DeclareMutating();

    auto mode = CheckedEnumCast<ELockMode>(request->mode());
    bool waitable = request->waitable();

    if (mode != ELockMode::Snapshot &&
        mode != ELockMode::Shared &&
        mode != ELockMode::Exclusive)
    {
        THROW_ERROR_EXCEPTION("Invalid lock mode %Qlv",
            mode);
    }

    TLockRequest lockRequest;
    if (request->has_child_key()) {
        if (mode != ELockMode::Shared) {
            THROW_ERROR_EXCEPTION("Only %Qlv locks are allowed on child keys, got %Qlv",
                ELockMode::Shared,
                mode);
        }
        lockRequest = TLockRequest::MakeSharedChild(request->child_key());
    } else if (request->has_attribute_key()) {
        if (mode != ELockMode::Shared) {
            THROW_ERROR_EXCEPTION("Only %Qlv locks are allowed on attribute keys, got %Qlv",
                ELockMode::Shared,
                mode);
        }
        lockRequest = TLockRequest::MakeSharedAttribute(request->attribute_key());
    } else {
        lockRequest = TLockRequest(mode);
    }

    lockRequest.Timestamp = request->timestamp();

    context->SetRequestInfo("Mode: %v, Key: %v, Waitable: %v",
        mode,
        lockRequest.Key,
        waitable);

    ValidateTransaction();
    ValidatePermission(
        EPermissionCheckScope::This,
        mode == ELockMode::Snapshot ? EPermission::Read : EPermission::Write);
    ValidateLockPossible();

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* lock = cypressManager->CreateLock(
        TrunkNode_,
        Transaction_,
        lockRequest,
        waitable);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto externalCellTag = TrunkNode_->IsExternal()
        ? TrunkNode_->GetExternalCellTag()
        : multicellManager->GetCellTag();

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    auto externalTransactionId = TrunkNode_->IsExternal()
        ? transactionManager->ExternalizeTransaction(Transaction_, externalCellTag)
        : Transaction_->GetId();

    auto lockId = lock->GetId();
    auto revision = TrunkNode_->GetRevision();

    ToProto(response->mutable_lock_id(), lockId);
    ToProto(response->mutable_node_id(), TrunkNode_->GetId());
    ToProto(response->mutable_external_transaction_id(), externalTransactionId);
    response->set_external_cell_tag(externalCellTag);
    response->set_revision(revision);

    context->SetResponseInfo("LockId: %v, ExternalCellTag: %v, ExternalTransactionId: %v, Revision: %llx",
        lockId,
        externalCellTag,
        externalTransactionId,
        revision);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Unlock)
{
    DeclareMutating();

    context->SetRequestInfo();

    if (!GetDynamicCypressManagerConfig()->EnableUnlockCommand) {
        THROW_ERROR_EXCEPTION("Unlock command is not enabled");
    }

    ValidateTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    cypressManager->UnlockNode(TrunkNode_, Transaction_);

    context->SetResponseInfo();

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Create)
{
    DeclareMutating();
    auto type = EObjectType(request->type());
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto recursive = request->recursive();
    auto force = request->force();
    auto ignoreTypeMismatch = request->ignore_type_mismatch();
    const auto& path = GetRequestTargetYPath(context->RequestHeader());

    YT_LOG_ACCESS_IF(
        IsAccessLoggedType(type),
        context,
        GetPath(),
        Transaction_,
        {{"type", FormatEnum(type)}});

    context->SetRequestInfo("Type: %v, IgnoreExisting: %v, LockExisting: %v, Recursive: %v, Force: %v, IgnoreTypeMismatch: %v",
        type,
        ignoreExisting,
        lockExisting,
        recursive,
        force,
        ignoreTypeMismatch);

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (!ignoreExisting && lockExisting) {
        THROW_ERROR_EXCEPTION("Cannot specify \"lock_existing\" without \"ignore_existing\"");
    }

    if (!ignoreExisting && ignoreTypeMismatch) {
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_type_mismatch\" without \"ignore_existing\"");
    }

    bool replace = path.empty();
    if (replace && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(this);
        }

        const auto* impl = GetThisImpl();
        if (impl->GetType() != type && !force && !ignoreTypeMismatch) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "%v already exists and has type %Qlv while node of %Qlv type is about to be created",
                GetPath(),
                impl->GetType(),
                type);
        }

        if (lockExisting) {
            LockThisImpl();
        }

        ToProto(response->mutable_node_id(), impl->GetId());
        response->set_cell_tag(impl->GetExternalCellTag() == NotReplicatedCellTag
            ? Bootstrap_->GetMulticellManager()->GetCellTag()
            : impl->GetExternalCellTag());
        context->SetResponseInfo("ExistingNodeId: %v",
            impl->GetId());
        context->Reply();
        return;
    }

    if (!replace && !CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }

    ICompositeNodePtr parent;
    if (replace) {
        parent = GetParent();
        if (!parent) {
            ThrowCannotReplaceNode(this);
        }
    }

    std::unique_ptr<IAttributeDictionary> explicitAttributes;
    if (request->has_node_attributes()) {
        explicitAttributes = FromProto(request->node_attributes());
    }

    ValidateCreatePermissions(replace, explicitAttributes.get());

    auto* node = GetThisImpl();
    // The node inside which the new node must be created.
    auto* intendedParentNode = replace ? node->GetParent() : node;
    auto* account = intendedParentNode->GetAccount();

    TInheritedAttributeDictionary inheritedAttributes(Bootstrap_);
    GatherInheritableAttributes(intendedParentNode, &inheritedAttributes.Attributes());

    if (explicitAttributes) {
        auto optionalAccount = explicitAttributes->FindAndRemove<TString>("account");
        if (optionalAccount) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            account = securityManager->GetAccountByNameOrThrow(*optionalAccount);
            account->ValidateActiveLifeStage();
        }
    }

    if (type == EObjectType::Link && explicitAttributes->Contains("target_path")) {
        auto targetPath = explicitAttributes->Get<TString>("target_path");
        YT_LOG_ACCESS(
            context,
            GetPath(),
            Transaction_,
            {{"destination_path", targetPath}},
            "Link");
    }

    auto factory = CreateCypressFactory(account, TNodeFactoryOptions());
    auto newProxy = factory->CreateNode(type, &inheritedAttributes, explicitAttributes.get());

    if (replace) {
        parent->ReplaceChild(this, newProxy);
    } else {
        SetChildNode(
            factory.get(),
            path,
            newProxy,
            recursive);
    }

    factory->Commit();

    auto* newNode = newProxy->GetTrunkNode();
    const auto& newNodeId = newNode->GetId();
    auto newNodeCellTag = newNode->GetExternalCellTag() == NotReplicatedCellTag
        ? Bootstrap_->GetMulticellManager()->GetCellTag()
        : newNode->GetExternalCellTag();

    ToProto(response->mutable_node_id(), newNode->GetId());
    response->set_cell_tag(newNodeCellTag);

    context->SetResponseInfo("NodeId: %v, CellTag: %v, Account: %v",
        newNodeId,
        newNodeCellTag,
        newNode->GetAccount()->GetName());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Copy)
{
    DeclareMutating();

    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    // COMPAT(babenko)
    const auto& sourcePath = ypathExt.additional_paths_size() == 1
        ? ypathExt.additional_paths(0)
        : request->source_path();
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto mode = CheckedEnumCast<ENodeCloneMode>(request->mode());

    if (!ignoreExisting && lockExisting) {
        THROW_ERROR_EXCEPTION("Cannot specify \"lock_existing\" without \"ignore_existing\"");
    }

    if (ignoreExisting && mode == ENodeCloneMode::Move) {
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_existing\" for move operation");
    }

    context->SetIncrementalRequestInfo("SourcePath: %v, Mode: %v",
        sourcePath,
        mode);

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto sourceProxy = cypressManager->ResolvePathToNodeProxy(sourcePath, Transaction_);

    auto* trunkSourceNode = sourceProxy->GetTrunkNode();

    if (trunkSourceNode == TrunkNode_) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to itself");
    }

    if (IsAncestorOf(trunkSourceNode, TrunkNode_)) {
        THROW_ERROR_EXCEPTION("Cannot copy or move a node to its descendant");
    }

    auto* sourceNode = (mode == ENodeCloneMode::Move)
        ? LockImpl(trunkSourceNode, ELockMode::Exclusive, true)
        : cypressManager->GetVersionedNode(trunkSourceNode, Transaction_);

    ValidateCopyFromSourcePermissions(sourceNode, mode);

    auto sourceParentProxy = sourceProxy->GetParent();
    if (!sourceParentProxy && mode == ENodeCloneMode::Move) {
        ThrowCannotRemoveNode(sourceProxy);
    }

    YT_LOG_ACCESS(
        context,
        sourceProxy->GetPath(),
        Transaction_,
        {{"destination_path", GetPath()}},
        mode == ENodeCloneMode::Move ? "Move" : "Copy");

    CopyCore(
        context,
        false,
        [&] (ICypressNodeFactory* factory) {
            return factory->CloneNode(sourceNode, mode);
        });

    if (mode == ENodeCloneMode::Move) {
        sourceParentProxy->RemoveChild(sourceProxy);
    }

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, BeginCopy)
{
    DeclareMutating();
    ValidateTransaction();

    auto mode = CheckedEnumCast<ENodeCloneMode>(request->mode());

    context->SetRequestInfo("Mode: %v",
        mode);

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& handler = cypressManager->GetHandler(TrunkNode_);

    auto* node = GetThisImpl();

    ValidatePermission(node, EPermissionCheckScope::This | EPermissionCheckScope::Descendants, EPermission::Read);

    TBeginCopyContext copyContext(
        Transaction_,
        mode,
        node);
    copyContext.SetVersion(NCellMaster::GetCurrentReign());
    handler->BeginCopy(node, &copyContext);

    ToProto(response->mutable_portal_child_ids(), copyContext.PortalRootIds());
    ToProto(response->mutable_external_cell_tags(), copyContext.GetExternalCellTags());

    auto uncompressedData = copyContext.Finish();
    auto codecId = GetDynamicCypressManagerConfig()->TreeSerializationCodec;
    auto* codec = NCompression::GetCodec(codecId);
    auto compressedData = codec->Compress(uncompressedData);

    auto* serializedTree = response->mutable_serialized_tree();
    serializedTree->set_version(copyContext.GetVersion());
    serializedTree->set_data(compressedData.begin(), compressedData.size());
    serializedTree->set_codec_id(static_cast<int>(codecId));

    ToProto(response->mutable_node_id(), GetId());
    ToProto(response->mutable_opaque_child_paths(), copyContext.OpaqueChildPaths());

    context->SetResponseInfo("Codec: %v, UncompressedDataSize: %v, CompressedDataSize: %v, ExternalCellTags: %v",
        codecId,
        GetByteSize(uncompressedData),
        compressedData.Size(),
        response->external_cell_tags());
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, EndCopy)
{
    DeclareMutating();
    ValidateTransaction();

    auto mode = CheckedEnumCast<ENodeCloneMode>(request->mode());
    bool inplace = request->inplace();
    const auto& serializedTree = request->serialized_tree();
    const auto& compressedData = serializedTree.data();
    auto codecId = CheckedEnumCast<NCompression::ECodec>(serializedTree.codec_id());

    if (serializedTree.version() != NCellMaster::GetCurrentReign()) {
        THROW_ERROR_EXCEPTION("Invalid tree format version: expected %v, actual %v",
            NCellMaster::GetCurrentReign(),
            serializedTree.version());
    }

    context->SetIncrementalRequestInfo("Codec: %v, CompressedDataSize: %v, Mode: %v, Inplace: %v",
        codecId,
        compressedData.size(),
        mode,
        inplace);

    auto* codec = NCompression::GetCodec(codecId);
    auto uncompressedData = codec->Decompress(TSharedRef::FromString(compressedData));

    TEndCopyContext copyContext(
        Bootstrap_,
        mode,
        uncompressedData);
    copyContext.SetVersion(serializedTree.version());

    CopyCore(
        context,
        inplace,
        [&] (ICypressNodeFactory* factory) {
            return inplace
                ? factory->EndCopyNodeInplace(TrunkNode_, &copyContext)
                : factory->EndCopyNode(&copyContext);
        });

    context->Reply();
}

template <class TContextPtr, class TClonedTreeBuilder>
void TNontemplateCypressNodeProxyBase::CopyCore(
    const TContextPtr& context,
    bool inplace,
    const TClonedTreeBuilder& clonedTreeBuilder)
{
    auto* request = &context->Request();
    auto* response = &context->Response();

    const auto& targetPath = GetRequestTargetYPath(context->RequestHeader());
    bool preserveAccount = request->preserve_account();
    bool preserveCreationTime = request->preserve_creation_time();
    bool preserveModificationTime = request->preserve_modification_time();
    bool preserveExpirationTime = request->preserve_expiration_time();
    bool preserveOwner = request->preserve_owner();
    bool preserveAcl = request->preserve_acl();
    auto recursive = request->recursive();
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto force = request->force();
    auto pessimisticQuotaCheck = request->pessimistic_quota_check();

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }
    if (inplace && !targetPath.empty()) {
        THROW_ERROR_EXCEPTION("Cannot inplace copy to missing node");
    }

    context->SetRequestInfo("TransactionId: %v, "
        "PreserveAccount: %v, PreserveCreationTime: %v, PreserveModificationTime: %v, PreserveExpirationTime: %v, "
        "PreserveOwner: %v, PreserveAcl: %v, Recursive: %v, IgnoreExisting: %v,  LockExisting: %v, "
        "Force: %v, PessimisticQuotaCheck: %v",
        NObjectServer::GetObjectId(Transaction_),
        preserveAccount,
        preserveCreationTime,
        preserveModificationTime,
        preserveExpirationTime,
        preserveOwner,
        preserveAcl,
        recursive,
        ignoreExisting,
        lockExisting,
        force,
        pessimisticQuotaCheck);

    bool replace = targetPath.empty();
    if (replace && !force && !inplace) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(this);
        }

        if (lockExisting) {
            LockThisImpl();
        }

        ToProto(response->mutable_node_id(), TrunkNode_->GetId());
        context->SetResponseInfo("ExistingNodeId: %v",
            TrunkNode_->GetId());
        return;
    }

    if (!replace && !CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }

    ICompositeNodePtr parentProxy;
    if (replace && !inplace) {
        parentProxy = GetParent();
        if (!parentProxy) {
            ThrowCannotReplaceNode(this);
        }
    }

    ValidateCopyToThisDestinationPermissions(replace && !inplace, preserveAcl);

    auto* account = (replace && !inplace)
        ? ICypressNodeProxy::FromNode(parentProxy.Get())->GetTrunkNode()->GetAccount()
        : GetThisImpl()->GetAccount();

    auto factory = CreateCypressFactory(account, TNodeFactoryOptions{
        .PreserveAccount = preserveAccount,
        .PreserveCreationTime = preserveCreationTime,
        .PreserveModificationTime = preserveModificationTime,
        .PreserveExpirationTime = preserveExpirationTime,
        .PreserveOwner = preserveOwner,
        .PreserveAcl = preserveAcl,
        .PessimisticQuotaCheck = pessimisticQuotaCheck
    });

    auto* clonedNode = clonedTreeBuilder(factory.get());
    auto* clonedTrunkNode = clonedNode->GetTrunkNode();
    if (!inplace) {
        auto clonedProxy = GetProxy(clonedTrunkNode);
        if (replace) {
            parentProxy->ReplaceChild(this, clonedProxy);
        } else {
            SetChildNode(
                factory.get(),
                targetPath,
                clonedProxy,
                recursive);
        }
    }

    factory->Commit();

    ToProto(response->mutable_node_id(), clonedTrunkNode->GetId());

    context->SetResponseInfo("NodeId: %v", clonedTrunkNode->GetId());
}

void TNontemplateCypressNodeProxyBase::ValidateAccessTransaction()
{
    if (Object_->IsNative()) {
        return;
    }

    if (!Transaction_) {
        return;
    }

    if (Object_->GetNativeCellTag() == Transaction_->GetNativeCellTag()) {
        return;
    }

    THROW_ERROR_EXCEPTION("Accessing a foreign object %v via transaction %v is not allowed",
        Object_->GetId(),
        Transaction_->GetId());
}

////////////////////////////////////////////////////////////////////////////////

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
    TNontemplateCypressNodeProxyBase::ListSystemAttributes(descriptors);

    const auto* node = GetThisImpl<TCompositeNodeBase>();

    descriptors->push_back(EInternedAttributeKey::Count);

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionCodec)
        .SetPresent(node->GetCompressionCodec().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ErasureCodec)
        .SetPresent(node->GetErasureCodec().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrimaryMedium)
        .SetPresent(node->GetPrimaryMediumIndex().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Media)
        .SetPresent(node->GetMedia().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Vital)
        .SetPresent(node->GetVital().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationFactor)
        .SetPresent(node->GetReplicationFactor().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
        .SetPresent(node->GetTabletCellBundle())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Atomicity)
        .SetPresent(node->GetAtomicity().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CommitOrdering)
        .SetPresent(node->GetCommitOrdering().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::InMemoryMode)
        .SetPresent(node->GetInMemoryMode().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OptimizeFor)
        .SetPresent(node->GetOptimizeFor().operator bool())
        .SetWritable(true)
        .SetRemovable(true));
}

bool TNontemplateCompositeCypressNodeProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* node = GetThisImpl<TCompositeNodeBase>();

    switch (key) {
        case EInternedAttributeKey::Count:
            BuildYsonFluently(consumer)
                .Value(GetChildCount());
            return true;

#define XX(camelCaseName, snakeCaseName) \
        case EInternedAttributeKey::camelCaseName: \
            if (!node->Get##camelCaseName()) { \
                break; \
            } \
            BuildYsonFluently(consumer) \
                .Value(node->Get##camelCaseName()); \
            return true; \

        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

        case EInternedAttributeKey::PrimaryMedium: {
            if (!node->GetPrimaryMediumIndex()) {
                break;
            }
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto* medium = chunkManager->GetMediumByIndex(*node->GetPrimaryMediumIndex());
            BuildYsonFluently(consumer)
                .Value(medium->GetName());
            return true;
        }

        case EInternedAttributeKey::Media: {
            if (!node->GetMedia()) {
                break;
            }
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            BuildYsonFluently(consumer)
                .Value(TSerializableChunkReplication(*node->GetMedia(), chunkManager));
            return true;
        }

        case EInternedAttributeKey::TabletCellBundle:
            if (!node->GetTabletCellBundle()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(node->GetTabletCellBundle()->GetName());
            return true;

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

bool TNontemplateCompositeCypressNodeProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value)
{
    auto* node = GetThisImpl<TCompositeNodeBase>();

    // Attributes "media", "primary_medium", "replication_factor" are interrelated
    // and nullable, which greatly complicates their modification.
    //
    // The rule of thumb is: if possible, consistency of non-null attributes is
    // checked, but an attribute is never required to be set just for the
    // purposes of validation of other attributes. For instance: "media" and
    // "replication_factor" are checked for consistency only when "primary_medium" is
    // set. Without it, it's impossible to tell which medium the "replication_factor"
    // pertains to, and these attributes may be modified virtually independently.

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto throwReplicationFactorMismatch = [&] (int mediumIndex) {
        const auto& medium = chunkManager->GetMediumByIndexOrThrow(mediumIndex);
        THROW_ERROR_EXCEPTION(
            "Attributes \"media\" and \"replication_factor\" have contradicting values for medium %Qv",
            medium->GetName());
    };

    switch (key) {
        case EInternedAttributeKey::PrimaryMedium: {
            ValidateNoTransaction();

            auto mediumName = ConvertTo<TString>(value);
            auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
            const auto mediumIndex = medium->GetIndex();
            const auto replication = node->GetMedia();

            if (!replication) {
                ValidatePermission(medium, EPermission::Use);
                node->SetPrimaryMediumIndex(mediumIndex);
                return true;
            }

            TChunkReplication newReplication;
            if (ValidatePrimaryMediumChange(
                medium,
                *replication,
                node->GetPrimaryMediumIndex(), // may be null
                &newReplication))
            {
                const auto replicationFactor = node->GetReplicationFactor();
                if (replicationFactor &&
                    *replicationFactor != newReplication.Get(mediumIndex).GetReplicationFactor())
                {
                    throwReplicationFactorMismatch(mediumIndex);
                }

                node->SetMedia(newReplication);
                node->SetPrimaryMediumIndex(mediumIndex);
            } // else no change is required

            return true;
        }

        case EInternedAttributeKey::Media: {
            ValidateNoTransaction();

            auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
            TChunkReplication replication;
            // Vitality isn't a part of TSerializableChunkReplication, assume true.
            replication.SetVital(true);
            serializableReplication.ToChunkReplication(&replication, chunkManager);

            const auto oldReplication = node->GetMedia();

            if (replication == oldReplication) {
                return true;
            }

            const auto primaryMediumIndex = node->GetPrimaryMediumIndex();
            const auto replicationFactor = node->GetReplicationFactor();
            if (primaryMediumIndex && replicationFactor) {
                if (replication.Get(*primaryMediumIndex).GetReplicationFactor() != *replicationFactor) {
                    throwReplicationFactorMismatch(*primaryMediumIndex);
                }
            }

            // NB: primary medium index may be null, in which case corresponding
            // parts of validation will be skipped.
            ValidateMediaChange(oldReplication, primaryMediumIndex, replication);
            node->SetMedia(replication);

            return true;
        }

        case EInternedAttributeKey::ReplicationFactor: {
            ValidateNoTransaction();

            auto replicationFactor = ConvertTo<int>(value);
            if (replicationFactor == node->GetReplicationFactor()) {
                return true;
            }

            ValidateReplicationFactor(replicationFactor);

            const auto mediumIndex = node->GetPrimaryMediumIndex();
            if (mediumIndex) {
                const auto replication = node->GetMedia();
                if (replication) {
                    if (replication->Get(*mediumIndex).GetReplicationFactor() != replicationFactor) {
                        throwReplicationFactorMismatch(*mediumIndex);
                    }
                } else if (!node->GetReplicationFactor()) {
                    auto* medium = chunkManager->GetMediumByIndex(*mediumIndex);
                    ValidatePermission(medium, EPermission::Use);
                }
            }

            node->SetReplicationFactor(replicationFactor);

            return true;
        }

        case EInternedAttributeKey::TabletCellBundle: {
            ValidateNoTransaction();

            auto name = ConvertTo<TString>(value);

            const auto& tabletManager = Bootstrap_->GetTabletManager();
            auto* newBundle = tabletManager->GetTabletCellBundleByNameOrThrow(name);
            newBundle->ValidateActiveLifeStage();

            tabletManager->SetTabletCellBundle(node, newBundle);

            return true;
        }

#define XX(camelCaseName, snakeCaseName) \
        case EInternedAttributeKey::camelCaseName: \
            ValidateNoTransaction(); \
            node->Set##camelCaseName(ConvertTo<decltype(node->Get##camelCaseName())>(value)); \
            return true; \

        // Can't use FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE here as
        // replication_factor is "simple" yet must be handled separately.
        XX(CompressionCodec, compression_codec)
        XX(ErasureCodec, erasure_codec)
        XX(Vital, vital)
        XX(Atomicity, atomicity)
        XX(CommitOrdering, commit_ordering)
        XX(InMemoryMode, in_memory_mode)
        XX(OptimizeFor, optimize_for)
#undef XX

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value);
}

bool TNontemplateCompositeCypressNodeProxyBase::RemoveBuiltinAttribute(TInternedAttributeKey key)
{
    auto* node = GetThisImpl<TCompositeNodeBase>();

    switch (key) {

#define XX(camelCaseName, snakeCaseName) \
        case EInternedAttributeKey::camelCaseName: \
            ValidateNoTransaction(); \
            node->Set##camelCaseName(std::nullopt); \
            return true; \

        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
        XX(Media, media);
#undef XX

        case EInternedAttributeKey::PrimaryMedium:
            ValidateNoTransaction();
            node->SetPrimaryMediumIndex(std::nullopt);
            return true;

        case EInternedAttributeKey::TabletCellBundle: {
            ValidateNoTransaction();

            auto* bundle = node->GetTabletCellBundle();
            if (bundle) {
                const auto& objectManager = Bootstrap_->GetObjectManager();
                objectManager->UnrefObject(bundle);
                node->SetTabletCellBundle(nullptr);
            }

            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::RemoveBuiltinAttribute(key);
}

// May return nullptr if there are no annotations available.
TCypressNode* TNontemplateCypressNodeProxyBase::FindClosestAncestorWithAnnotation(TCypressNode* node)
{
    auto* result = node;
    while (result && !result->GetAnnotation()) {
        result = result->GetParent();
    }
    return result;
}

bool TNontemplateCompositeCypressNodeProxyBase::CanHaveChildren() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TInheritedAttributeDictionary::TInheritedAttributeDictionary(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

std::vector<TString> TInheritedAttributeDictionary::ListKeys() const
{
    std::vector<TString> result;
#define XX(camelCaseName, snakeCaseName) \
    if (InheritedAttributes_.camelCaseName) { \
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

std::vector<IAttributeDictionary::TKeyValuePair> TInheritedAttributeDictionary::ListPairs() const
{
    return ListAttributesPairs(*this);
}

TYsonString TInheritedAttributeDictionary::FindYson(TStringBuf key) const
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        const auto& value = InheritedAttributes_.camelCaseName; \
        return value ? ConvertToYsonString(*value) : TYsonString(); \
    }

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);

#undef XX

    if (key == "primary_medium") {
        const auto& primaryMediumIndex = InheritedAttributes_.PrimaryMediumIndex;
        if (!primaryMediumIndex) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*primaryMediumIndex);
        return ConvertToYsonString(medium->GetName());
    }

    if (key == "media") {
        const auto& replication = InheritedAttributes_.Media;
        if (!replication) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return ConvertToYsonString(TSerializableChunkReplication(*replication, chunkManager));
    }

    if (key == "tablet_cell_bundle") {
        auto* tabletCellBundle = InheritedAttributes_.TabletCellBundle;
        if (!tabletCellBundle) {
            return {};
        }
        return ConvertToYsonString(tabletCellBundle->GetName());
    }

    return Fallback_ ? Fallback_->FindYson(key) : TYsonString();
}

void TInheritedAttributeDictionary::SetYson(const TString& key, const TYsonString& value)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        auto& attr = InheritedAttributes_.camelCaseName; \
        using TAttr = std::remove_reference<decltype(*attr)>::type; \
        attr = ConvertTo<TAttr>(value); \
        return; \
    }

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);

#undef XX

    if (key == "primary_medium") {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& mediumName = ConvertTo<TString>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        InheritedAttributes_.PrimaryMediumIndex = medium->GetIndex();
        return;
    }

    if (key == "media") {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        InheritedAttributes_.Media = replication;
        return;
    }

    if (key == "tablet_cell_bundle") {
        auto bundleName = ConvertTo<TString>(value);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* bundle = tabletManager->GetTabletCellBundleByNameOrThrow(bundleName);
        bundle->ValidateActiveLifeStage();
        InheritedAttributes_.TabletCellBundle = bundle;
        return;
    }

    if (!Fallback_) {
        Fallback_ = CreateEphemeralAttributes();
    }

    Fallback_->SetYson(key, value);
}

bool TInheritedAttributeDictionary::Remove(const TString& key)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        if (InheritedAttributes_.camelCaseName) { \
            InheritedAttributes_.camelCaseName = decltype(InheritedAttributes_.camelCaseName)(); \
        } \
        return true; \
    }

    FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (Fallback_) {
        return Fallback_->Remove(key);
    }

    return false;
}

TCompositeNodeBase::TAttributes& TInheritedAttributeDictionary::Attributes()
{
    return InheritedAttributes_;
}

////////////////////////////////////////////////////////////////////////////////

void TMapNodeProxy::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    context->SetRequestInfo();
    TMapNodeMixin::SetRecursive(path, request, response, context);
}

void TMapNodeProxy::Clear()
{
    // Take shared lock for the node itself.
    auto* impl = LockThisImpl(ELockMode::Shared);

    // Construct children list.
    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TMapNode>(),
        Transaction_,
        &keyToChildMapStorage);
    auto keyToChildList = SortHashMapByKeys(keyToChildMap);

    // Take shared locks for children.
    typedef std::pair<TString, TCypressNode*> TChild;
    std::vector<TChild> children;
    children.reserve(keyToChildList.size());
    for (const auto& pair : keyToChildList) {
        LockThisImpl(TLockRequest::MakeSharedChild(pair.first));
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
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto originators = cypressManager->GetNodeOriginators(Transaction_, TrunkNode_);

    int result = 0;
    for (const auto* node : originators) {
        const auto* mapNode = node->As<TMapNode>();
        result += mapNode->ChildCountDelta();

        if (mapNode->GetLockMode() == ELockMode::Snapshot) {
            break;
        }
    }
    return result;
}

std::vector<std::pair<TString, INodePtr>> TMapNodeProxy::GetChildren() const
{
    THashMap<TString, TCypressNode*> keyToChildStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TMapNode>(),
        Transaction_,
        &keyToChildStorage);

    std::vector<std::pair<TString, INodePtr>> result;
    result.reserve(keyToChildMap.size());
    for (const auto& pair : keyToChildMap) {
        result.push_back(std::make_pair(pair.first, GetProxy(pair.second)));
    }

    return result;
}

std::vector<TString> TMapNodeProxy::GetKeys() const
{
    THashMap<TString, TCypressNode*> keyToChildStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TMapNode>(),
        Transaction_,
        &keyToChildStorage);

    std::vector<TString> result;
    for (const auto& pair : keyToChildMap) {
        result.push_back(pair.first);
    }

    return result;
}

INodePtr TMapNodeProxy::FindChild(const TString& key) const
{
    auto* trunkChildNode = FindMapNodeChild(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TMapNode>(),
        Transaction_,
        key);
    return trunkChildNode ? GetProxy(trunkChildNode) : nullptr;
}

bool TMapNodeProxy::AddChild(const TString& key, const NYTree::INodePtr& child)
{
    YT_ASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    auto* impl = LockThisImpl(TLockRequest::MakeSharedChild(key));
    auto* trunkChildImpl = ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode();
    auto* childImpl = LockImpl(trunkChildImpl);

    const auto& objectManager = Bootstrap_->GetObjectManager();

    auto& children = impl->MutableChildren(objectManager);
    children.Set(objectManager, key, trunkChildImpl);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(TrunkNode_);

    ++impl->ChildCountDelta();

    AttachChild(TrunkNode_, childImpl);

    SetModified();

    return true;
}

bool TMapNodeProxy::RemoveChild(const TString& key)
{
    auto* trunkChildImpl = FindMapNodeChild(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TMapNode>(),
        Transaction_,
        key);
    if (!trunkChildImpl) {
        return false;
    }

    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);
    auto* impl = LockThisImpl(TLockRequest::MakeSharedChild(key));
    DoRemoveChild(impl, key, childImpl);

    SetModified();

    return true;
}

void TMapNodeProxy::RemoveChild(const INodePtr& child)
{
    auto optionalKey = FindChildKey(child);
    if (!optionalKey) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    const auto& key = *optionalKey;

    auto* trunkChildImpl = ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode();

    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);
    auto* impl = LockThisImpl(TLockRequest::MakeSharedChild(key));
    DoRemoveChild(impl, key, childImpl);

    SetModified();
}

void TMapNodeProxy::ReplaceChild(const INodePtr& oldChild, const INodePtr& newChild)
{
    if (oldChild == newChild) {
        return;
    }

    auto optionalKey = FindChildKey(oldChild);
    if (!optionalKey) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    const auto& key = *optionalKey;

    auto* oldTrunkChildImpl = ICypressNodeProxy::FromNode(oldChild.Get())->GetTrunkNode();
    auto* oldChildImpl = LockImpl(oldTrunkChildImpl, ELockMode::Exclusive, true);

    auto* newTrunkChildImpl = ICypressNodeProxy::FromNode(newChild.Get())->GetTrunkNode();
    auto* newChildImpl = LockImpl(newTrunkChildImpl);

    auto* impl = LockThisImpl(TLockRequest::MakeSharedChild(key));

    const auto& objectManager = Bootstrap_->GetObjectManager();

    auto& children = impl->MutableChildren(objectManager);

    DetachChild(TrunkNode_, oldChildImpl);
    children.Set(objectManager, key, newTrunkChildImpl);
    AttachChild(TrunkNode_, newChildImpl);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(impl);

    SetModified();
}

std::optional<TString> TMapNodeProxy::FindChildKey(const IConstNodePtr& child)
{
    return FindNodeKey(
        Bootstrap_->GetCypressManager(),
        ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode(),
        Transaction_);
}

bool TMapNodeProxy::DoInvoke(const NRpc::IServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    return TBase::DoInvoke(context);
}

void TMapNodeProxy::SetChildNode(
    INodeFactory* factory,
    const TYPath& path,
    const INodePtr& child,
    bool recursive)
{
    TMapNodeMixin::SetChild(
        factory,
        path,
        child,
        recursive);
}

int TMapNodeProxy::GetMaxChildCount() const
{
    return GetDynamicCypressManagerConfig()->MaxNodeChildCount;
}

int TMapNodeProxy::GetMaxKeyLength() const
{
    return GetDynamicCypressManagerConfig()->MaxMapNodeKeyLength;
}

IYPathService::TResolveResult TMapNodeProxy::ResolveRecursive(
    const TYPath& path,
    const IServiceContextPtr& context)
{
    return TMapNodeMixin::ResolveRecursive(path, context);
}

void TMapNodeProxy::DoRemoveChild(
    TMapNode* impl,
    const TString& key,
    TCypressNode* childImpl)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto* trunkChildImpl = childImpl->GetTrunkNode();
    auto& children = impl->MutableChildren(objectManager);
    if (Transaction_) {
        children.Set(objectManager, key, nullptr);
    } else {
        children.Remove(objectManager, key, trunkChildImpl);
    }
    DetachChild(TrunkNode_, childImpl);
    --impl->ChildCountDelta();

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(impl);
}

void TMapNodeProxy::ListSelf(
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto attributeKeys = request->has_attributes()
        ? std::make_optional(FromProto<std::vector<TString>>(request->attributes().keys()))
        : std::nullopt;

    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("AttributeKeys: %v, Limit: %v",
        attributeKeys,
        limit);

    TAsyncYsonWriter writer;

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& securityManager = Bootstrap_->GetSecurityManager();

    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        TrunkNode_->As<TMapNode>(),
        Transaction_,
        &keyToChildMapStorage);

    if (limit && keyToChildMap.size() > *limit) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    i64 counter = 0;

    writer.OnBeginList();
    for (const auto& pair : keyToChildMap) {
        const auto& key = pair.first;
        auto* trunkChild  = pair.second;
        writer.OnListItem();

        if (CheckItemReadPermissions(TrunkNode_, trunkChild, securityManager)) {
            auto proxy = cypressManager->GetNodeProxy(trunkChild, Transaction_);
            proxy->WriteAttributes(&writer, attributeKeys, false);
        }

        writer.OnStringScalar(key);

        if (limit && ++counter >= *limit) {
            break;
        }
    }
    writer.OnEndList();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().GetData());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

void TListNodeProxy::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    context->SetRequestInfo();
    TListNodeMixin::SetRecursive(path, request, response, context);
}

void TListNodeProxy::Clear()
{
    auto* impl = LockThisImpl();

    // Lock children and collect impls.
    std::vector<TCypressNode*> children;
    for (auto* trunkChild : impl->IndexToChild()) {
        children.push_back(LockImpl(trunkChild));
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    // Detach children.
    for (auto* child : children) {
        DetachChild(TrunkNode_, child);
        objectManager->UnrefObject(child->GetTrunkNode());
    }

    impl->IndexToChild().clear();
    impl->ChildToIndex().clear();

    SetModified();
}

int TListNodeProxy::GetChildCount() const
{
    const auto* impl = GetThisImpl();
    return impl->IndexToChild().size();
}

std::vector<INodePtr> TListNodeProxy::GetChildren() const
{
    std::vector<INodePtr> result;
    const auto* impl = GetThisImpl();
    const auto& indexToChild = impl->IndexToChild();
    result.reserve(indexToChild.size());
    for (auto* child : indexToChild) {
        result.push_back(GetProxy(child));
    }
    return result;
}

INodePtr TListNodeProxy::FindChild(int index) const
{
    const auto* impl = GetThisImpl();
    const auto& indexToChild = impl->IndexToChild();
    return index >= 0 && index < indexToChild.size() ? GetProxy(indexToChild[index]) : nullptr;
}

void TListNodeProxy::AddChild(const INodePtr& child, int beforeIndex /*= -1*/)
{
    auto* impl = LockThisImpl();
    auto& list = impl->IndexToChild();

    auto* trunkChildImpl = ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode();
    auto* childImpl = LockImpl(trunkChildImpl);

    if (beforeIndex < 0) {
        YT_VERIFY(impl->ChildToIndex().insert(std::make_pair(trunkChildImpl, static_cast<int>(list.size()))).second);
        list.push_back(trunkChildImpl);
    } else {
        // Update indices.
        for (auto it = list.begin() + beforeIndex; it != list.end(); ++it) {
            ++impl->ChildToIndex()[*it];
        }

        // Insert the new child.
        YT_VERIFY(impl->ChildToIndex().insert(std::make_pair(trunkChildImpl, beforeIndex)).second);
        list.insert(list.begin() + beforeIndex, trunkChildImpl);
    }

    AttachChild(TrunkNode_, childImpl);
    const auto& objectManager = Bootstrap_->GetObjectManager();
    objectManager->RefObject(childImpl->GetTrunkNode());

    SetModified();
}

bool TListNodeProxy::RemoveChild(int index)
{
    auto* impl = LockThisImpl();
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
    YT_VERIFY(impl->ChildToIndex().erase(trunkChildImpl));
    DetachChild(TrunkNode_, childImpl);
    const auto& objectManager = Bootstrap_->GetObjectManager();
    objectManager->UnrefObject(childImpl->GetTrunkNode());

    SetModified();
    return true;
}

void TListNodeProxy::RemoveChild(const INodePtr& child)
{
    int index = GetChildIndexOrThrow(child);
    YT_VERIFY(RemoveChild(index));
}

void TListNodeProxy::ReplaceChild(const INodePtr& oldChild, const INodePtr& newChild)
{
    if (oldChild == newChild)
        return;

    auto* impl = LockThisImpl();

    auto* oldTrunkChildImpl = ICypressNodeProxy::FromNode(oldChild.Get())->GetTrunkNode();
    auto* oldChildImpl = LockImpl(oldTrunkChildImpl);

    auto* newTrunkChildImpl = ICypressNodeProxy::FromNode(newChild.Get())->GetTrunkNode();
    auto* newChildImpl = LockImpl(newTrunkChildImpl);

    auto it = impl->ChildToIndex().find(oldTrunkChildImpl);
    YT_ASSERT(it != impl->ChildToIndex().end());

    int index = it->second;

    const auto& objectManager = Bootstrap_->GetObjectManager();
    DetachChild(TrunkNode_, oldChildImpl);
    objectManager->UnrefObject(oldChildImpl->GetTrunkNode());

    impl->IndexToChild()[index] = newTrunkChildImpl;
    impl->ChildToIndex().erase(it);
    YT_VERIFY(impl->ChildToIndex().insert(std::make_pair(newTrunkChildImpl, index)).second);
    AttachChild(TrunkNode_, newChildImpl);
    objectManager->RefObject(newChildImpl->GetTrunkNode());

    SetModified();
}

std::optional<int> TListNodeProxy::FindChildIndex(const IConstNodePtr& child)
{
    const auto* impl = GetThisImpl();

    auto* trunkChildImpl = ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode();

    auto it = impl->ChildToIndex().find(trunkChildImpl);
    return it == impl->ChildToIndex().end() ? std::nullopt : std::make_optional(it->second);
}

void TListNodeProxy::SetChildNode(
    INodeFactory* factory,
    const TYPath& path,
    const INodePtr& child,
    bool recursive)
{
    TListNodeMixin::SetChild(
        factory,
        path,
        child,
        recursive);
}

int TListNodeProxy::GetMaxChildCount() const
{
    return GetDynamicCypressManagerConfig()->MaxNodeChildCount;
}

IYPathService::TResolveResult TListNodeProxy::ResolveRecursive(
    const TYPath& path,
    const IServiceContextPtr& context)
{
    return TListNodeMixin::ResolveRecursive(path, context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
