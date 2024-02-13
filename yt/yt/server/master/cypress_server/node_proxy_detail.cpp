#include "node_proxy_detail.h"
#include "private.h"
#include "helpers.h"
#include "shard.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>
#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/security_server/access_log.h>
#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/config.h>
#include <yt/yt/server/master/security_server/helpers.h>
#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/tablet_server/tablet_cell_bundle.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/transaction_server/config.h>

#include <yt/yt/server/master/chaos_server/chaos_cell_bundle.h>
#include <yt/yt/server/master/chaos_server/chaos_manager.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node_detail.h>
#include <yt/yt/core/ytree/request_complexity_limiter.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/compression/codec.h>

#include <type_traits>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NLogging;
using namespace NYson;
using namespace NYPath;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NTabletServer;
using namespace NChaosServer;
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
        "BeginCopy"
    };
    return methodsForAccessLog.contains(method);
}

bool HasTrivialAcd(const TCypressNode* node)
{
    const auto& acd = node->Acd();
    return acd.Inherit() && acd.Acl().Entries.empty();
}

bool CheckItemReadPermissions(
    TCypressNode* parent,
    TCypressNode* child,
    const ISecurityManagerPtr& securityManager)
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
            .SetDetailedMasterMemory(EMasterMemoryType::Attributes, 1);
        securityManager->ValidateResourceUsageIncrease(node->Account().Get(), resourceUsageIncrease);
    }

    auto* userAttributes = node->GetMutableAttributes();
    const auto& ysonInternRegistry = Proxy_->Bootstrap_->GetYsonInternRegistry();
    userAttributes->Set(key, ysonInternRegistry->Intern(value));

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

TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TCypressNode* trunkNode)
    : TObjectProxyBase(bootstrap, metadata, trunkNode)
    , THierarchicPermissionValidator(CreatePermissionValidator())
    , CustomAttributesImpl_(New<TCustomAttributeDictionary>(this))
    , Transaction_(transaction)
    , TrunkNode_(trunkNode)
    , VersionedId_(Object_->GetId(), GetObjectId(Transaction_))
{
    YT_ASSERT(TrunkNode_);
    YT_ASSERT(TrunkNode_->IsTrunk());

    CustomAttributes_ = CustomAttributesImpl_.Get();
}

std::unique_ptr<ITransactionalNodeFactory> TNontemplateCypressNodeProxyBase::CreateFactory() const
{
    auto* account = GetThisImpl()->Account().Get();
    return CreateCypressFactory(account, TNodeFactoryOptions(), /*unresolvedPathSuffix*/ TYPath());
}

std::unique_ptr<ICypressNodeFactory> TNontemplateCypressNodeProxyBase::CreateCypressFactory(
    TAccount* account,
    const TNodeFactoryOptions& options,
    TYPath unresolvedPathSuffix) const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& serviceTrunkNode = GetThisImpl()->GetTrunkNode();
    return cypressManager->CreateNodeFactory(
        serviceTrunkNode->GetShard(),
        Transaction_,
        account,
        options,
        serviceTrunkNode,
        std::move(unresolvedPathSuffix));
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
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            return cypressManager->ComputeRecursiveResourceUsage(GetTrunkNode(), GetTransaction());
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

    auto proxy = CreateObjectServiceReadProxy(
        Bootstrap_->GetRootClient(),
        NApi::EMasterChannelKind::Follower,
        externalCellTag);
    return proxy.Execute(req).Apply(BIND([=, this, this_ = MakeStrong(this)] (const TYPathProxy::TErrorOrRspGetPtr& rspOrError) {
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

bool TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    switch (key) {
        case EInternedAttributeKey::Account: {
            ValidateNoTransaction();

            const auto& securityManager = Bootstrap_->GetSecurityManager();

            auto name = ConvertTo<TString>(value);
            auto* account = securityManager->GetAccountByNameOrThrow(name, true /*activeLifeStageOnly*/);

            ValidateStorageParametersUpdate();
            ValidatePermission(account, EPermission::Use);

            auto* node = LockThisImpl();
            if (node->Account() != account) {
                // TODO(savrus) See YT-7050
                securityManager->ValidateResourceUsageIncrease(account, TClusterResources().SetNodeCount(1));
                securityManager->SetAccount(node, account, /*transaction*/ nullptr);
            }

            SetModified(EModificationType::Attributes);

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

        case EInternedAttributeKey::ExpirationTimeout: {
            ValidatePermission(EPermissionCheckScope::This|EPermissionCheckScope::Descendants, EPermission::Remove);

            const auto& cypressManager = Bootstrap_->GetCypressManager();

            if (TrunkNode_ == cypressManager->GetRootNode()) {
                THROW_ERROR_EXCEPTION("Cannot set \"expiration_timeout\" for the root");
            }

            auto timeout = ConvertTo<TDuration>(value);

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);

            cypressManager->SetExpirationTimeout(node, timeout);

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

        case EInternedAttributeKey::Annotation: {
            auto annotation = ConvertTo<std::optional<TString>>(value);
            if (annotation) {
                ValidateAnnotation(*annotation);
            }
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedNode = LockThisImpl(lockRequest);
            if (annotation) {
                lockedNode->SetAnnotation(*annotation);
            } else {
                lockedNode->RemoveAnnotation();
            }
            return true;
        }

        default:
            break;
    }

    return TObjectProxyBase::SetBuiltinAttribute(key, value, force);
}

bool TNontemplateCypressNodeProxyBase::RemoveBuiltinAttribute(TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::Annotation: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedNode = LockThisImpl(lockRequest);
            lockedNode->RemoveAnnotation();
            return true;
        }

        case EInternedAttributeKey::ExpirationTime: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTime(node, std::nullopt);

            return true;
        }

        case EInternedAttributeKey::ExpirationTimeout: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTimeout(node, std::nullopt);

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

void TNontemplateCypressNodeProxyBase::LogAcdUpdate(TInternedAttributeKey key, const TYsonString& value)
{
    TObjectProxyBase::LogAcdUpdate(key, value);

    if (!GetThisImpl()->IsBeingCreated()) {
        NSecurityServer::LogAcdUpdate(key.Unintern(), GetPath(), value);
    }
}

TVersionedObjectId TNontemplateCypressNodeProxyBase::GetVersionedId() const
{
    return VersionedId_;
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
        .SetPresent(node->TryGetExpirationTime().has_value())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTimeout)
        .SetPresent(node->TryGetExpirationTimeout().has_value())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EffectiveExpiration)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::CreationTime);
    descriptors->push_back(EInternedAttributeKey::ModificationTime);
    descriptors->push_back(EInternedAttributeKey::AccessTime);
    descriptors->push_back(EInternedAttributeKey::AccessCounter);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::NativeContentRevision)
        .SetExternal(isExternal));
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
    auto isExternal = node->IsExternal();
    auto isNative = node->IsNative();

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

        case EInternedAttributeKey::ExpirationTimeout: {
            auto optionalExpirationTimeout = node->TryGetExpirationTimeout();
            if (!optionalExpirationTimeout) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*optionalExpirationTimeout);
            return true;
        }

        case EInternedAttributeKey::EffectiveExpiration: {
            const auto& cypressManager = Bootstrap_->GetCypressManager();

            auto fluent = BuildYsonFluently(consumer).BeginMap();

            if (auto* effectiveNode = node->GetEffectiveExpirationTimeNode()) {
                fluent.Item("time")
                    .DoMap([&] (NYTree::TFluentMap fluent) {
                        fluent
                            .Item("value").Value(*effectiveNode->TryGetExpirationTime())
                            .Item("path").Value(cypressManager->GetNodePath(effectiveNode, GetTransaction()));
                    });
            } else {
                fluent.Item("time").Entity();
            }

            if (auto* effectiveNode = node->GetEffectiveExpirationTimeoutNode()) {
                fluent.Item("timeout")
                    .DoMap([&] (NYTree::TFluentMap fluent) {
                        fluent
                            .Item("value").Value(*effectiveNode->TryGetExpirationTimeout())
                            .Item("path").Value(cypressManager->GetNodePath(effectiveNode, GetTransaction()));
                    });
            } else {
                fluent.Item("timeout").Entity();
            }

            fluent.EndMap();
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

        case EInternedAttributeKey::AttributeRevision:
            BuildYsonFluently(consumer)
                .Value(node->GetAttributeRevision());
            return true;

        case EInternedAttributeKey::ContentRevision:
            BuildYsonFluently(consumer)
                .Value(node->GetContentRevision());
            return true;

        case EInternedAttributeKey::NativeContentRevision:
            if (isExternal || isNative) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(node->GetNativeContentRevision());
            return true;

        case EInternedAttributeKey::ResourceUsage: {
            auto resourceUsage = GetNodeResourceUsage(node);
            SerializeRichClusterResources(
                resourceUsage,
                consumer,
                Bootstrap_);
            return true;
        }

        case EInternedAttributeKey::Account:
            BuildYsonFluently(consumer)
                .Value(node->Account()->GetName());
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
            if (auto annotation = GetEffectiveAnnotation(node)) {
                BuildYsonFluently(consumer)
                    .Value(*annotation);
            } else {
                BuildYsonFluently(consumer)
                    .Entity();
            }
            return true;
        }

        case EInternedAttributeKey::AnnotationPath: {
            if (const auto* annotationNode = FindClosestAncestorWithAnnotation(node)) {
                const auto& cypressManager = Bootstrap_->GetCypressManager();
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(annotationNode->GetTrunkNode(), GetTransaction()));
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

void TNontemplateCypressNodeProxyBase::GetBasicAttributes(TGetBasicAttributesContext* context)
{
    TObjectProxyBase::GetBasicAttributes(context);

    auto* node = GetThisImpl();

    // Base class method has already filled these but we need to look at the branch.
    context->Revision = node->GetRevision();
    context->AttributeRevision = node->GetAttributeRevision();
    context->ContentRevision = node->GetContentRevision();
}

void TNontemplateCypressNodeProxyBase::ValidateMethodWhitelistedForTransaction(const TString& method) const
{
    const auto& transactionManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->TransactionManager;
    if (!Transaction_ || !transactionManagerConfig->CheckTransactionIsCompatibleWithMethod) {
        return;
    }

    const auto& typeToWhitelist = transactionManagerConfig->TransactionTypeToMethodWhitelist;
    auto transactionType = TypeFromId(Transaction_->GetId());
    auto it = typeToWhitelist.find(transactionType);
    if (it != typeToWhitelist.end() && !it->second.contains(method)) {
        THROW_ERROR_EXCEPTION("Method %Qv is not supported for type %Qlv",
            method,
            transactionType)
            << TErrorAttribute("transaction_id", Transaction_->GetId());
    }
}

void TNontemplateCypressNodeProxyBase::BeforeInvoke(const IYPathServiceContextPtr& context)
{
    AccessTrackingSuppressed_ = GetSuppressAccessTracking(context->RequestHeader());
    ExpirationTimeoutRenewalSuppressed_ = GetSuppressExpirationTimeoutRenewal(context->RequestHeader());
    ValidateMethodWhitelistedForTransaction(context->GetMethod());

    TObjectProxyBase::BeforeInvoke(context);
}

void TNontemplateCypressNodeProxyBase::AfterInvoke(const IYPathServiceContextPtr& context)
{
    SetAccessed();
    SetTouched();
    TObjectProxyBase::AfterInvoke(context);
}

bool TNontemplateCypressNodeProxyBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    ValidateAccessTransaction();

    auto doInvoke = [&] (const IYPathServiceContextPtr& context) {
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
    };

    auto path = YT_EVALUATE_FOR_ACCESS_LOG_IF(
        IsAccessLoggedMethod(context->GetMethod()),
        GetPath());

    auto result = doInvoke(context);

    YT_LOG_ACCESS_IF(
        IsAccessLoggedMethod(context->GetMethod()),
        context,
        GetId(),
        path,
        Transaction_);

    return result;
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
            ICypressManagerPtr cypressManager,
            ISecurityManagerPtr securityManager,
            TTransaction* transaction,
            TAttributeFilter attributeFilter,
            TReadRequestComplexityLimiterPtr complexityLimiter)
            : CypressManager_(std::move(cypressManager))
            , SecurityManager_(std::move(securityManager))
            , Transaction_(transaction)
            , AttributeFilter_(std::move(attributeFilter))
            , Writer_(std::move(complexityLimiter))
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
        const ICypressManagerPtr CypressManager_;
        const ISecurityManagerPtr SecurityManager_;
        TTransaction* const Transaction_;
        const TAttributeFilter AttributeFilter_;

        TLimitedAsyncYsonWriter Writer_;

        void VisitAny(TCypressNode* trunkParent, TCypressNode* trunkChild)
        {
            if (!CheckItemReadPermissions(trunkParent, trunkChild, SecurityManager_)) {
                Writer_.OnEntity();
                return;
            }

            auto proxy = CypressManager_->GetNodeProxy(trunkChild, Transaction_);
            proxy->WriteAttributes(&Writer_, AttributeFilter_, false);

            if (trunkParent && trunkChild->GetOpaque()) {
                Writer_.OnEntity();
                return;
            }

            switch (trunkChild->GetNodeType()) {
                case ENodeType::List:
                    VisitList(trunkChild->As<TListNode>());
                    break;
                case ENodeType::Map:
                    VisitMap(trunkChild->As<TCypressMapNode>());
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
                node->As<TCypressMapNode>(),
                Transaction_,
                &keyToChildMapStorage);
            for (const auto& [key, child] : keyToChildMap) {
                Writer_.OnKeyedItem(key);
                VisitAny(node, child);
            }
            Writer_.OnEndMap();
        }
    };

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    // TODO(babenko): make use of limit
    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("AttributeFilter: %v, Limit: %v",
        attributeFilter,
        limit);

    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    TVisitor visitor(
        Bootstrap_->GetCypressManager(),
        Bootstrap_->GetSecurityManager(),
        Transaction_,
        std::move(attributeFilter),
        context->GetReadRequestComplexityLimiter());
    visitor.Run(TrunkNode_);
    visitor.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().ToString());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TNontemplateCypressNodeProxyBase::DoRemoveSelf(bool recursive, bool force)
{
    auto* node = GetThisImpl();

    if (node->GetType() == EObjectType::PortalExit || node->GetType() == EObjectType::Scion) {
        // TODO(babenko, gritukan)
        if (Transaction_) {
            THROW_ERROR_EXCEPTION("Removing %v in transaction is not supported",
                node->GetLowercaseObjectName());
        }
        YT_VERIFY(node->IsTrunk());

        LockImpl(node, ELockMode::Exclusive, true);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(node);
    } else {
        TNodeBase::DoRemoveSelf(recursive, force);
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
    const TLockRequest& request,
    bool recursive)
{
    CachedNode_ = LockImpl(TrunkNode_, request, recursive);
    YT_ASSERT(CachedNode_->GetTransaction() == Transaction_);
    return CachedNode_;
}

ICypressNodeProxyPtr TNontemplateCypressNodeProxyBase::GetProxy(TCypressNode* trunkNode) const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    return cypressManager->GetNodeProxy(trunkNode, Transaction_);
}

void TNontemplateCypressNodeProxyBase::ValidatePermission(
    EPermissionCheckScope scope,
    EPermission permission,
    const TString& /*user*/)
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

TCompactVector<TCypressNode*, 1> TNontemplateCypressNodeProxyBase::ListDescendantsForPermissionValidation(TCypressNode* node)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* trunkNode = node->GetTrunkNode();
    return cypressManager->ListSubtreeNodes(trunkNode, Transaction_, false);
}

TCypressNode* TNontemplateCypressNodeProxyBase::GetParentForPermissionValidation(TCypressNode* node)
{
    return node->GetParent();
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

    if (!IsObjectAlive(TrunkNode_)) {
        return;
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    if (!CachedNode_) {
        CachedNode_ = cypressManager->GetNode(GetVersionedId());
    }

    cypressManager->SetModified(CachedNode_, modificationType);

    // NB: not calling base class method here.
}

void TNontemplateCypressNodeProxyBase::SetAccessed()
{
    if (AccessTrackingSuppressed_) {
        return;
    }

    if (!IsObjectAlive(TrunkNode_)) {
        return;
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    cypressManager->SetAccessed(TrunkNode_);
}

void TNontemplateCypressNodeProxyBase::SuppressAccessTracking()
{
    AccessTrackingSuppressed_ = true;
}

void TNontemplateCypressNodeProxyBase::SetTouched()
{
    if (ExpirationTimeoutRenewalSuppressed_) {
        return;
    }

    if (!IsObjectAlive(TrunkNode_)) {
        return;
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    cypressManager->SetTouched(TrunkNode_);
}

void TNontemplateCypressNodeProxyBase::SuppressExpirationTimeoutRenewal()
{
    ExpirationTimeoutRenewalSuppressed_ = true;
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
    auto lockResult = cypressManager->CreateLock(
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
        ? transactionManager->ExternalizeTransaction(Transaction_, {externalCellTag})
        : Transaction_->GetId();

    auto lockId = lockResult.Lock->GetId();
    auto revision = lockResult.BranchedNode ? lockResult.BranchedNode->GetRevision() : NHydra::NullRevision;

    ToProto(response->mutable_lock_id(), lockId);
    ToProto(response->mutable_node_id(), TrunkNode_->GetId());
    ToProto(response->mutable_external_transaction_id(), externalTransactionId);
    response->set_external_cell_tag(ToProto<int>(externalCellTag));
    response->set_revision(revision);

    context->SetResponseInfo("LockId: %v, ExternalCellTag: %v, ExternalTransactionId: %v, Revision: %x",
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
    auto hintId = FromProto<TNodeId>(request->hint_id());

    context->SetRequestInfo(
        "Type: %v, IgnoreExisting: %v, LockExisting: %v, Recursive: %v, "
        "Force: %v, IgnoreTypeMismatch: %v, HintId: %v",
        type,
        ignoreExisting,
        lockExisting,
        recursive,
        force,
        ignoreTypeMismatch,
        hintId);

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (!ignoreExisting && lockExisting) {
        THROW_ERROR_EXCEPTION("Cannot specify \"lock_existing\" without \"ignore_existing\"");
    }

    if (!ignoreExisting && ignoreTypeMismatch) {
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_type_mismatch\" without \"ignore_existing\"");
    }

    if (Transaction_) {
        auto transactionId = Transaction_->GetId();
        const auto& transactionManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->TransactionManager;
        if (transactionManagerConfig->CheckTransactionIsCompatibleWithMethod &&
            IsSystemTransactionType(TypeFromId(transactionId)) &&
            type != EObjectType::ChaosReplicatedTable)
        {
            THROW_ERROR_EXCEPTION("Cannot create type %Qlv using system transaction", type)
                << TErrorAttribute("transaction_id", transactionId);
        }
    }

    bool replace = path.empty();
    if (replace && !force) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(this);
        }

        const auto* impl = GetThisImpl();
        // Existing Portal instead of MapNode is ok when ignore_existing is set.
        auto compatibleTypes = type == EObjectType::MapNode && impl->GetType() == EObjectType::PortalExit;
        if (impl->GetType() != type && !force && !ignoreTypeMismatch && !compatibleTypes) {
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
        response->set_cell_tag(ToProto<int>(impl->GetExternalCellTag() == NotReplicatedCellTagSentinel
            ? Bootstrap_->GetMulticellManager()->GetCellTag()
            : impl->GetExternalCellTag()));
        context->SetResponseInfo("ExistingNodeId: %v",
            impl->GetId());
        context->Reply();

        YT_LOG_ACCESS_IF(
            IsAccessLoggedType(type),
            context,
            impl->GetId(),
            GetPath(),
            Transaction_,
            {{"existing", "true"}});

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

    IAttributeDictionaryPtr explicitAttributes;
    if (request->has_node_attributes()) {
        explicitAttributes = FromProto(request->node_attributes());
    }

    ValidateCreatePermissions(replace, explicitAttributes.Get());

    auto* node = GetThisImpl();
    // The node inside which the new node must be created.
    auto* intendedParentNode = replace ? node->GetParent() : node;
    auto* account = intendedParentNode->Account().Get();

    auto inheritedAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
    GatherInheritableAttributes(
        intendedParentNode,
        &inheritedAttributes->Attributes());

    std::optional<TYPath> optionalTargetPath;
    if (explicitAttributes) {
        optionalTargetPath = explicitAttributes->Find<TYPath>("target_path");

        auto optionalAccount = explicitAttributes->FindAndRemove<TString>("account");
        if (optionalAccount) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            account = securityManager->GetAccountByNameOrThrow(*optionalAccount, true /*activeLifeStageOnly*/);
        }
    }

    auto factory = CreateCypressFactory(account, TNodeFactoryOptions(), path);
    auto newProxy = factory->CreateNode(
        type,
        hintId,
        inheritedAttributes.Get(),
        explicitAttributes.Get());

    // The path may be invalidated below; save it.
    auto thisNodePath = YT_EVALUATE_FOR_ACCESS_LOG_IF(IsAccessLoggedType(type), GetPath());

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
    auto newNodeId = newNode->GetId();
    auto newNodeCellTag = newNode->GetExternalCellTag() == NotReplicatedCellTagSentinel
        ? Bootstrap_->GetMulticellManager()->GetCellTag()
        : newNode->GetExternalCellTag();

    if (type == EObjectType::Link && optionalTargetPath) {
        YT_LOG_ACCESS(
            context,
            newNodeId,
            thisNodePath,
            Transaction_,
            {{"destination_path", *optionalTargetPath}},
            "Link");
    } else {
        YT_LOG_ACCESS_IF(
            IsAccessLoggedType(type),
            context,
            newNodeId,
            thisNodePath,
            Transaction_);
    }

    ToProto(response->mutable_node_id(), newNode->GetId());
    response->set_cell_tag(ToProto<int>(newNodeCellTag));

    context->SetResponseInfo("NodeId: %v, CellTag: %v, Account: %v",
        newNodeId,
        newNodeCellTag,
        newNode->Account()->GetName());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Copy)
{
    DeclareMutating();

    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    if (ypathExt.additional_paths_size() != 1) {
        THROW_ERROR_EXCEPTION("Invalid number of additional paths");
    }
    const auto& originalSourcePath = ypathExt.additional_paths(0);

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
        originalSourcePath,
        mode);

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto sourceProxy = cypressManager->ResolvePathToNodeProxy(originalSourcePath, Transaction_);

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

    // The path may be invalidated by removal below; save it.
    auto sourcePath = YT_EVALUATE_FOR_ACCESS_LOG(sourceProxy->GetPath());

    TNodeId clonedTrunkNodeId;
    CopyCore(
        context,
        false,
        [&] (ICypressNodeFactory* factory) {
            auto* clonedNode = factory->CloneNode(sourceNode, mode);
            auto* clonedTrunkNode = clonedNode->GetTrunkNode();
            clonedTrunkNodeId = clonedTrunkNode->GetId();
            return clonedNode;
        });

    if (mode == ENodeCloneMode::Move) {
        sourceParentProxy->RemoveChild(sourceProxy);
    }

    sourceProxy->SetAccessed();
    sourceProxy->SetTouched();

    YT_LOG_ACCESS(
        context,
        sourceProxy->GetId(),
        sourcePath,
        Transaction_,
        {{"destination_id", ToString(clonedTrunkNodeId)},
         {"destination_path", GetPath()}},
        mode == ENodeCloneMode::Move ? "Move" : "Copy");

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

    ValidatePermission(node, EPermissionCheckScope::This | EPermissionCheckScope::Descendants, EPermission::FullRead);

    TBeginCopyContext copyContext(
        Transaction_,
        mode,
        node);
    handler->BeginCopy(node, &copyContext);

    const auto& schemaMap = copyContext.GetRegisteredSchemas();
    std::vector<std::pair<TMasterTableSchema*, TEntitySerializationKey>> schemas{
        schemaMap.begin(),
        schemaMap.end()
    };

    SortBy(schemas, [] (const auto& pair) {
        return pair.first->GetId();
    });

    for (auto [schema, key] : schemas) {
        auto* entry = response->add_schemas();
        entry->set_key(key.Underlying());
        ToProto(entry->mutable_schema(), *schema->AsTableSchema());
    }

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

    const auto& tableManager = Bootstrap_->GetTableManager();
    for (const auto& registeredSchema : request->schemas()) {
        auto key = TEntitySerializationKey(registeredSchema.key());
        auto tableSchema = FromProto<TTableSchema>(registeredSchema.schema());
        auto* schema = tableManager->GetOrCreateNativeMasterTableSchema(tableSchema, Transaction_);
        copyContext.RegisterSchema(key, schema);
    }

    TNodeId clonedTrunkNodeId;
    CopyCore(
        context,
        inplace,
        [&] (ICypressNodeFactory* factory) {
            auto* clonedNode = inplace
                ? factory->EndCopyNodeInplace(TrunkNode_, &copyContext)
                : factory->EndCopyNode(&copyContext);
            auto* clonedTrunkNode = clonedNode->GetTrunkNode();
            clonedTrunkNodeId = clonedTrunkNode->GetId();
            return clonedNode;
        });

    auto clonedNodeType = TypeFromId(clonedTrunkNodeId);
    YT_LOG_ACCESS_IF(
        IsAccessLoggedType(clonedNodeType),
        context,
        clonedTrunkNodeId,
        GetPath(),
        Transaction_);

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
    bool preserveExpirationTimeout = request->preserve_expiration_timeout();
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
        "PreserveAccount: %v, PreserveCreationTime: %v, PreserveModificationTime: %v, "
        "PreserveExpirationTime: %v, PreserveExpirationTimeout: %v, "
        "PreserveOwner: %v, PreserveAcl: %v, Recursive: %v, IgnoreExisting: %v,  LockExisting: %v, "
        "Force: %v, PessimisticQuotaCheck: %v",
        NObjectServer::GetObjectId(Transaction_),
        preserveAccount,
        preserveCreationTime,
        preserveModificationTime,
        preserveExpirationTime,
        preserveExpirationTimeout,
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
        ? ICypressNodeProxy::FromNode(parentProxy.Get())->GetTrunkNode()->Account().Get()
        : GetThisImpl()->Account().Get();

    const auto& path = GetRequestTargetYPath(context->RequestHeader());

    auto factory = CreateCypressFactory(
        account,
        TNodeFactoryOptions{
            .PreserveAccount = preserveAccount,
            .PreserveCreationTime = preserveCreationTime,
            .PreserveModificationTime = preserveModificationTime,
            .PreserveExpirationTime = preserveExpirationTime,
            .PreserveExpirationTimeout = preserveExpirationTimeout,
            .PreserveOwner = preserveOwner,
            .PreserveAcl = preserveAcl,
            .PessimisticQuotaCheck = pessimisticQuotaCheck
        },
        path);

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

    auto hasInheritableAttributes = node->HasInheritableAttributes();

    const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ChunkMerger; \

#define XX(camelCaseName, snakeCaseName) \
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::camelCaseName) \
        .SetPresent(hasInheritableAttributes && node->Has##camelCaseName()) \
        .SetWritable(true) \
        .SetRemovable(true)); \
    \
    if (EInternedAttributeKey::camelCaseName == EInternedAttributeKey::ChunkMergerMode && \
        !config->AllowSettingChunkMergerMode) \
    { \
        descriptors->back().SetWritePermission(EPermission::Administer); \
    } \

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PrimaryMedium)
        .SetPresent(hasInheritableAttributes && node->HasPrimaryMediumIndex())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Media)
        .SetPresent(hasInheritableAttributes && node->HasMedia())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
        .SetPresent(hasInheritableAttributes && node->HasTabletCellBundle())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChaosCellBundle)
        .SetPresent(hasInheritableAttributes && node->HasChaosCellBundle())
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
        case EInternedAttributeKey::camelCaseName: { \
            auto value = node->TryGet##camelCaseName(); \
            if (!value) { \
                break; \
            } \
            BuildYsonFluently(consumer) \
                .Value(*value); \
            return true; \
        }

        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

        case EInternedAttributeKey::PrimaryMedium: {
            auto optionalPrimaryMediumIndex = node->TryGetPrimaryMediumIndex();
            if (!optionalPrimaryMediumIndex) {
                break;
            }
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto* medium = chunkManager->GetMediumByIndex(*optionalPrimaryMediumIndex);
            BuildYsonFluently(consumer)
                .Value(medium->GetName());
            return true;
        }

        case EInternedAttributeKey::Media: {
            auto optionalMedia = node->TryGetMedia();
            if (!optionalMedia) {
                break;
            }
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            BuildYsonFluently(consumer)
                .Value(TSerializableChunkReplication(*optionalMedia, chunkManager));
            return true;
        }

        case EInternedAttributeKey::TabletCellBundle: {
            auto optionalBundle = node->TryGetTabletCellBundle();
            if (!optionalBundle) {
                break;
            }
            const auto& bundle = *optionalBundle;
            YT_VERIFY(bundle);
            BuildYsonFluently(consumer)
                .Value(bundle->GetName());
            return true;
        }

        case EInternedAttributeKey::ChaosCellBundle: {
            auto optionalBundle = node->TryGetChaosCellBundle();
            if (!optionalBundle) {
                break;
            }
            const auto& bundle = *optionalBundle;
            YT_VERIFY(bundle);
            BuildYsonFluently(consumer)
                .Value(bundle->GetName());
            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

bool TNontemplateCompositeCypressNodeProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
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

    switch (key) {
        case EInternedAttributeKey::PrimaryMedium: {
            auto primaryMediumName = ConvertTo<TString>(value);
            SetPrimaryMedium(primaryMediumName);
            return true;
        }

        case EInternedAttributeKey::Media: {
            auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
            SetMedia(serializableReplication);
            return true;
        }

        case EInternedAttributeKey::TabletCellBundle: {
            ValidateNoTransaction();

            auto name = ConvertTo<TString>(value);

            const auto& tabletManager = Bootstrap_->GetTabletManager();
            auto* newBundle = tabletManager->GetTabletCellBundleByNameOrThrow(name, true /*activeLifeStageOnly*/);
            node->SetTabletCellBundle(TTabletCellBundlePtr(newBundle));

            return true;
        }

        case EInternedAttributeKey::ChaosCellBundle: {
            ValidateNoTransaction();

            auto name = ConvertTo<TString>(value);

            const auto& chaosManager = Bootstrap_->GetChaosManager();
            auto* newBundle = chaosManager->GetChaosCellBundleByNameOrThrow(name, true /*activeLifeStageOnly*/);
            node->SetChaosCellBundle(TChaosCellBundlePtr(newBundle));

            return true;
        }

#define XX(camelCaseName, snakeCaseName) \
        case EInternedAttributeKey::camelCaseName: \
            if (key == EInternedAttributeKey::ReplicationFactor) { \
                auto replicationFactor = ConvertTo<int>(value); \
                SetReplicationFactor(replicationFactor); \
                return true; \
            } \
            \
            if (key == EInternedAttributeKey::CompressionCodec) { \
                const auto& chunkManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager; \
                ValidateCompressionCodec( \
                    value, \
                    chunkManagerConfig->DeprecatedCodecIds, \
                    chunkManagerConfig->DeprecatedCodecNameToAlias); \
            } \
            { \
                auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern()); \
                auto* lockedNode = LockThisImpl<TCompositeNodeBase>(lockRequest); \
                using TAttr = decltype(std::declval<TCompositeNodeBase::TPersistentAttributes>().camelCaseName)::TValue; \
                lockedNode->Set##camelCaseName(ConvertTo<TAttr>(value)); \
            } \
            return true; \

        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value, force);
}

void TNontemplateCompositeCypressNodeProxyBase::SetReplicationFactor(int replicationFactor)
{
    ValidateNoTransaction();

    auto* node = GetThisImpl<TCompositeNodeBase>();

    if (replicationFactor == node->TryGetReplicationFactor()) {
        return;
    }

    ValidateReplicationFactor(replicationFactor);

    const auto mediumIndex = node->TryGetPrimaryMediumIndex();
    if (mediumIndex) {
        const auto replication = node->TryGetMedia();
        if (replication) {
            if (replication->Get(*mediumIndex).GetReplicationFactor() != replicationFactor) {
                ThrowReplicationFactorMismatch(*mediumIndex);
            }
        } else if (!node->TryGetReplicationFactor()) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto* medium = chunkManager->GetMediumByIndex(*mediumIndex);
            ValidatePermission(medium, EPermission::Use);
        }
    }

    node->SetReplicationFactor(replicationFactor);
}

void TNontemplateCompositeCypressNodeProxyBase::SetPrimaryMedium(const TString& primaryMediumName)
{
    ValidateNoTransaction();

    auto* node = GetThisImpl<TCompositeNodeBase>();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto* medium = chunkManager->GetMediumByNameOrThrow(primaryMediumName);
    const auto mediumIndex = medium->GetIndex();
    const auto replication = node->TryGetMedia();

    if (!replication) {
        ValidatePermission(medium, EPermission::Use);
        node->SetPrimaryMediumIndex(mediumIndex);
        return;
    }

    TChunkReplication newReplication;
    if (ValidatePrimaryMediumChange(
            medium,
            *replication,
            node->TryGetPrimaryMediumIndex(), // may be null
            &newReplication))
    {
        const auto replicationFactor = node->TryGetReplicationFactor();
        if (replicationFactor &&
            *replicationFactor != newReplication.Get(mediumIndex).GetReplicationFactor())
        {
            ThrowReplicationFactorMismatch(mediumIndex);
        }

        node->SetMedia(newReplication);
        node->SetPrimaryMediumIndex(mediumIndex);
    } // else no change is required
}

void TNontemplateCompositeCypressNodeProxyBase::SetMedia(TSerializableChunkReplication serializableReplication)
{
    ValidateNoTransaction();

    auto* node = GetThisImpl<TCompositeNodeBase>();
    const auto& chunkManager = Bootstrap_->GetChunkManager();


    TChunkReplication replication;
    // Vitality isn't a part of TSerializableChunkReplication, assume true.
    replication.SetVital(true);
    serializableReplication.ToChunkReplication(&replication, chunkManager);

    const auto oldReplication = node->TryGetMedia();

    if (replication == oldReplication) {
        return;
    }

    const auto primaryMediumIndex = node->TryGetPrimaryMediumIndex();
    const auto replicationFactor = node->TryGetReplicationFactor();
    if (primaryMediumIndex && replicationFactor) {
        if (replication.Get(*primaryMediumIndex).GetReplicationFactor() != *replicationFactor) {
            ThrowReplicationFactorMismatch(*primaryMediumIndex);
        }
    }

    // NB: primary medium index may be null, in which case corresponding
    // parts of validation will be skipped.
    ValidateMediaChange(oldReplication, primaryMediumIndex, replication);
    node->SetMedia(replication);
}

void TNontemplateCompositeCypressNodeProxyBase::ThrowReplicationFactorMismatch(int mediumIndex) const
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& medium = chunkManager->GetMediumByIndexOrThrow(mediumIndex);
    THROW_ERROR_EXCEPTION(
        "Attributes \"media\" and \"replication_factor\" have contradicting values for medium %Qv",
        medium->GetName());
}

bool TNontemplateCompositeCypressNodeProxyBase::RemoveBuiltinAttribute(TInternedAttributeKey key)
{
    auto* node = GetThisImpl<TCompositeNodeBase>();

    switch (key) {

#define XX(camelCaseName, snakeCaseName) \
        case EInternedAttributeKey::camelCaseName: { \
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern()); \
            auto* lockedNode = LockThisImpl<TCompositeNodeBase>(lockRequest); \
            lockedNode->Remove##camelCaseName(); \
            return true; \
        }

        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
#undef XX

        case EInternedAttributeKey::Media:
            ValidateNoTransaction();
            node->RemoveMedia();
            return true;

        case EInternedAttributeKey::PrimaryMedium:
            ValidateNoTransaction();
            node->RemovePrimaryMediumIndex();
            return true;

        case EInternedAttributeKey::TabletCellBundle: {
            ValidateNoTransaction();
            node->RemoveTabletCellBundle();

            return true;
        }

        case EInternedAttributeKey::ChaosCellBundle: {
            ValidateNoTransaction();
            node->RemoveChaosCellBundle();
            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::RemoveBuiltinAttribute(key);
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

std::vector<IAttributeDictionary::TKeyValuePair> TInheritedAttributeDictionary::ListPairs() const
{
    return ListAttributesPairs(*this);
}

TYsonString TInheritedAttributeDictionary::FindYson(TStringBuf key) const
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        auto optionalValue = InheritedAttributes_.camelCaseName.ToOptional(); \
        return optionalValue ? ConvertToYsonString(*optionalValue) : TYsonString(); \
    } \

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
#undef XX

    if (key == "primary_medium") {
        auto optionalPrimaryMediumIndex = InheritedAttributes_.PrimaryMediumIndex.ToOptional();
        if (!optionalPrimaryMediumIndex) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(*optionalPrimaryMediumIndex);
        return ConvertToYsonString(medium->GetName());
    }

    if (key == "media") {
        auto optionalReplication = InheritedAttributes_.Media.ToOptional();
        if (!optionalReplication) {
            return {};
        }
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return ConvertToYsonString(TSerializableChunkReplication(*optionalReplication, chunkManager));
    }

    if (key == "tablet_cell_bundle") {
        auto optionalCellBundle = InheritedAttributes_.TabletCellBundle.ToOptional();
        if (!optionalCellBundle) {
            return {};
        }
        YT_VERIFY(*optionalCellBundle);
        return ConvertToYsonString((*optionalCellBundle)->GetName());
    }

    if (key == "chaos_cell_bundle") {
        auto optionalCellBundle = InheritedAttributes_.ChaosCellBundle.ToOptional();
        if (!optionalCellBundle) {
            return {};
        }
        YT_VERIFY(*optionalCellBundle);
        return ConvertToYsonString((*optionalCellBundle)->GetName());
    }

    return Fallback_ ? Fallback_->FindYson(key) : TYsonString();
}

void TInheritedAttributeDictionary::SetYson(const TString& key, const TYsonString& value)
{
#define XX(camelCaseName, snakeCaseName) \
    if (key == #snakeCaseName) { \
        if (key == "compression_codec") { \
            const auto& chunkManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager; \
            ValidateCompressionCodec( \
                value, \
                chunkManagerConfig->DeprecatedCodecIds, \
                chunkManagerConfig->DeprecatedCodecNameToAlias); \
        } \
        using TAttr = decltype(InheritedAttributes_.camelCaseName)::TValue; \
        InheritedAttributes_.camelCaseName.Set(ConvertTo<TAttr>(value)); \
        return; \
    }

    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    if (key == "primary_medium") {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& mediumName = ConvertTo<TString>(value);
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        InheritedAttributes_.PrimaryMediumIndex.Set(medium->GetIndex());
        return;
    }

    if (key == "media") {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto serializableReplication = ConvertTo<TSerializableChunkReplication>(value);
        TChunkReplication replication;
        replication.SetVital(true);
        serializableReplication.ToChunkReplication(&replication, chunkManager);
        InheritedAttributes_.Media.Set(replication);
        return;
    }

    if (key == "tablet_cell_bundle") {
        auto bundleName = ConvertTo<TString>(value);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* bundle = tabletManager->GetTabletCellBundleByNameOrThrow(bundleName, true /*activeLifeStageOnly*/);
        InheritedAttributes_.TabletCellBundle.Set(bundle);
        return;
    }

    if (key == "chaos_cell_bundle") {
        auto bundleName = ConvertTo<TString>(value);
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

bool TInheritedAttributeDictionary::Remove(const TString& key)
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

TCompositeNodeBase::TTransientAttributes& TInheritedAttributeDictionary::Attributes()
{
    return InheritedAttributes_;
}

////////////////////////////////////////////////////////////////////////////////

void TSupportsForcefulSetSelfMixin::ValidateSetSelf(bool force) const
{
    if (!force) {
        THROW_ERROR_EXCEPTION("\"set\" command without \"force\" flag is forbidden; use \"create\" instead");
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCypressMapNodeProxy::SetRecursive(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    context->SetRequestInfo();
    TMapNodeMixin::SetRecursive(path, request, response, context);
}

void TCypressMapNodeProxy::Clear()
{
    // Take shared lock for the node itself.
    auto* impl = LockThisImpl(ELockMode::Shared);

    // Construct children list.
    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildMapStorage);
    auto keyToChildList = SortHashMapByKeys(keyToChildMap);

    // Take shared locks for children.
    using TChild = std::pair<TString, TCypressNode*>;
    std::vector<TChild> children;
    children.reserve(keyToChildList.size());
    for (const auto& [key, child] : keyToChildList) {
        LockThisImpl(TLockRequest::MakeSharedChild(key));
        auto* childImpl = LockImpl(child);
        children.push_back(std::pair(key, childImpl));
    }

    // Insert tombstones (if in transaction).
    for (const auto& [key, child] : children) {
        DoRemoveChild(impl, key, child);
    }

    SetModified(EModificationType::Content);
}

int TCypressMapNodeProxy::GetChildCount() const
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto originators = cypressManager->GetNodeOriginators(Transaction_, TrunkNode_);

    int result = 0;
    for (const auto* node : originators) {
        const auto* mapNode = node->As<TCypressMapNode>();
        result += mapNode->ChildCountDelta();

        if (mapNode->GetLockMode() == ELockMode::Snapshot) {
            break;
        }
    }
    return result;
}

std::vector<std::pair<TString, INodePtr>> TCypressMapNodeProxy::GetChildren() const
{
    THashMap<TString, TCypressNode*> keyToChildStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildStorage);

    std::vector<std::pair<TString, INodePtr>> result;
    result.reserve(keyToChildMap.size());
    for (const auto& [key, child] : keyToChildMap) {
        result.emplace_back(key, GetProxy(child));
    }

    return result;
}

std::vector<TString> TCypressMapNodeProxy::GetKeys() const
{
    THashMap<TString, TCypressNode*> keyToChildStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildStorage);

    std::vector<TString> result;
    for (const auto& [key, child] : keyToChildMap) {
        result.push_back(key);
    }

    return result;
}

INodePtr TCypressMapNodeProxy::FindChild(const TString& key) const
{
    auto* trunkChildNode = FindMapNodeChild(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        key);
    return trunkChildNode ? GetProxy(trunkChildNode) : nullptr;
}

bool TCypressMapNodeProxy::AddChild(const TString& key, const NYTree::INodePtr& child)
{
    YT_ASSERT(!key.empty());

    if (FindChild(key)) {
        return false;
    }

    auto* impl = LockThisImpl(TLockRequest::MakeSharedChild(key));
    auto* trunkChildImpl = ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode();
    auto* childImpl = LockImpl(trunkChildImpl);

    auto& children = impl->MutableChildren();
    children.Set(key, trunkChildImpl);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(TrunkNode_);

    ++impl->ChildCountDelta();

    AttachChild(TrunkNode_, childImpl);

    SetModified(EModificationType::Content);

    return true;
}

bool TCypressMapNodeProxy::RemoveChild(const TString& key)
{
    auto* trunkChildImpl = FindMapNodeChild(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        key);
    if (!trunkChildImpl) {
        return false;
    }

    auto* childImpl = LockImpl(trunkChildImpl, ELockMode::Exclusive, true);
    auto* impl = LockThisImpl(TLockRequest::MakeSharedChild(key));
    DoRemoveChild(impl, key, childImpl);

    SetModified(EModificationType::Content);

    return true;
}

void TCypressMapNodeProxy::RemoveChild(const INodePtr& child)
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

    SetModified(EModificationType::Content);
}

void TCypressMapNodeProxy::ReplaceChild(const INodePtr& oldChild, const INodePtr& newChild)
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

    auto& children = impl->MutableChildren();

    DetachChild(TrunkNode_, oldChildImpl);
    children.Set(key, newTrunkChildImpl);
    AttachChild(TrunkNode_, newChildImpl);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(impl);

    SetModified(EModificationType::Content);
}

std::optional<TString> TCypressMapNodeProxy::FindChildKey(const IConstNodePtr& child)
{
    return FindNodeKey(
        Bootstrap_->GetCypressManager(),
        ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode(),
        Transaction_);
}

void TCypressMapNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    if (Bootstrap_->GetConfig()->ExposeTestingFacilities) {
        descriptors->push_back(EInternedAttributeKey::CoWCookie);
    }
}

bool TCypressMapNodeProxy::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    switch (key) {
        case EInternedAttributeKey::CoWCookie: {
            if (!Bootstrap_->GetConfig()->ExposeTestingFacilities) {
                break;
            }
            const auto* node = GetThisImpl();
            BuildYsonFluently(consumer)
                .Value(node->GetMapNodeChildrenAddress());
            return true;
        }

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TCypressMapNodeProxy::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(List);
    return TBase::DoInvoke(context);
}

void TCypressMapNodeProxy::SetChildNode(
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

int TCypressMapNodeProxy::GetMaxChildCount() const
{
    return GetDynamicCypressManagerConfig()->MaxNodeChildCount;
}

int TCypressMapNodeProxy::GetMaxKeyLength() const
{
    return GetDynamicCypressManagerConfig()->MaxMapNodeKeyLength;
}

IYPathService::TResolveResult TCypressMapNodeProxy::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    return TMapNodeMixin::ResolveRecursive(path, context);
}

void TCypressMapNodeProxy::DoRemoveChild(
    TCypressMapNode* impl,
    const TString& key,
    TCypressNode* childImpl)
{
    auto* trunkChildImpl = childImpl->GetTrunkNode();
    auto& children = impl->MutableChildren();
    if (Transaction_) {
        children.Set(key, nullptr);
    } else {
        children.Remove(key, trunkChildImpl);
    }
    DetachChild(TrunkNode_, childImpl);
    --impl->ChildCountDelta();

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(impl);
}

void TCypressMapNodeProxy::ListSelf(
    TReqList* request,
    TRspList* response,
    const TCtxListPtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    YT_LOG_ACCESS(
        context,
        GetId(),
        GetPath(),
        Transaction_);

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    auto limit = request->has_limit()
        ? std::make_optional(request->limit())
        : std::nullopt;

    context->SetRequestInfo("AttributeFilter: %v, Limit: %v",
        attributeFilter,
        limit);

    TLimitedAsyncYsonWriter writer(context->GetReadRequestComplexityLimiter());

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& securityManager = Bootstrap_->GetSecurityManager();

    THashMap<TString, TCypressNode*> keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        cypressManager,
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildMapStorage);

    if (limit && std::ssize(keyToChildMap) > *limit) {
        writer.OnBeginAttributes();
        writer.OnKeyedItem("incomplete");
        writer.OnBooleanScalar(true);
        writer.OnEndAttributes();
    }

    i64 counter = 0;

    writer.OnBeginList();
    for (const auto& [key, trunkChild] : keyToChildMap) {
        writer.OnListItem();

        if (CheckItemReadPermissions(TrunkNode_, trunkChild, securityManager)) {
            auto proxy = cypressManager->GetNodeProxy(trunkChild, Transaction_);
            proxy->WriteAttributes(&writer, attributeFilter, false);
        }

        writer.OnStringScalar(key);

        if (limit && ++counter >= *limit) {
            break;
        }
    }
    writer.OnEndList();

    writer.Finish()
        .Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
            if (resultOrError.IsOK()) {
                response->set_value(resultOrError.Value().ToString());
                context->Reply();
            } else {
                context->Reply(resultOrError);
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

void TSequoiaMapNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    if (Bootstrap_->GetConfig()->ExposeTestingFacilities) {
        descriptors->push_back(EInternedAttributeKey::Children);
    }
}

bool TSequoiaMapNodeProxy::GetBuiltinAttribute(
    NYTree::TInternedAttributeKey key,
    NYson::IYsonConsumer* consumer)
{
    auto* mapNode = GetThisImpl();
    const auto Logger = CypressServerLogger;

    switch (key) {
        case EInternedAttributeKey::Type:
            BuildYsonFluently(consumer)
                .Value(EObjectType::MapNode);
            return true;
        case EInternedAttributeKey::Path:
            if (mapNode->ImmutableSequoiaProperties()) {
                BuildYsonFluently(consumer)
                    .Value(mapNode->ImmutableSequoiaProperties()->Path);
                return true;
            }
            YT_LOG_ALERT("Sequoia node is lacking required attribute \"path\" (NodeId: %v)",
                mapNode->GetId());
            return false;
        case EInternedAttributeKey::Key:
            if (mapNode->ImmutableSequoiaProperties()) {
                BuildYsonFluently(consumer)
                    .Value(mapNode->ImmutableSequoiaProperties()->Key);
                return true;
            }
            YT_LOG_ALERT("Sequoia node is lacking required attribute \"key\" (NodeId: %v)",
                mapNode->GetId());
            return false;
        case EInternedAttributeKey::Children: {
            if (!Bootstrap_->GetConfig()->ExposeTestingFacilities) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(mapNode->KeyToChild());
            return true;
        }
        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

void TSequoiaMapNodeProxy::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    // NB: Since Sequoia tree cannot be traversed on master side (due to the fact that nodes live on different cells),
    // limit field in request does nothing.
    context->SetRequestInfo("AttributeFilter: %v",
        attributeFilter);

    TLimitedAsyncYsonWriter writer(context->GetReadRequestComplexityLimiter());
    WriteAttributes(&writer, attributeFilter, false);
    writer.OnBeginMap();
    writer.OnEndMap();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(resultOrError.Value().ToString());
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

int TSequoiaMapNodeProxy::GetChildCount() const
{
    const auto* node = GetThisImpl();
    const auto* mapNode = node->As<TSequoiaMapNode>();
    return mapNode->KeyToChild().size();
}

void TSequoiaMapNodeProxy::Clear()
{
    YT_ABORT();
}

void TSequoiaMapNodeProxy::ReplaceChild(const NYTree::INodePtr& /*oldChild*/, const NYTree::INodePtr& /*newChild*/)
{
    YT_ABORT();
}

void TSequoiaMapNodeProxy::RemoveChild(const NYTree::INodePtr& /*child*/)
{
    YT_ABORT();
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

    SetModified(EModificationType::Content);
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
    return index >= 0 && index < std::ssize(indexToChild) ? GetProxy(indexToChild[index]) : nullptr;
}

void TListNodeProxy::AddChild(const INodePtr& child, int beforeIndex /*= -1*/)
{
    auto* impl = LockThisImpl();
    auto& list = impl->IndexToChild();

    auto* trunkChildImpl = ICypressNodeProxy::FromNode(child.Get())->GetTrunkNode();
    auto* childImpl = LockImpl(trunkChildImpl);

    if (beforeIndex < 0) {
        YT_VERIFY(impl->ChildToIndex().emplace(trunkChildImpl, static_cast<int>(list.size())).second);
        list.push_back(trunkChildImpl);
    } else {
        // Update indices.
        for (auto it = list.begin() + beforeIndex; it != list.end(); ++it) {
            ++impl->ChildToIndex()[*it];
        }

        // Insert the new child.
        YT_VERIFY(impl->ChildToIndex().emplace(trunkChildImpl, beforeIndex).second);
        list.insert(list.begin() + beforeIndex, trunkChildImpl);
    }

    AttachChild(TrunkNode_, childImpl);
    const auto& objectManager = Bootstrap_->GetObjectManager();
    objectManager->RefObject(childImpl->GetTrunkNode());

    SetModified(EModificationType::Content);
}

bool TListNodeProxy::RemoveChild(int index)
{
    auto* impl = LockThisImpl();
    auto& list = impl->IndexToChild();

    if (index < 0 || index >= std::ssize(list)) {
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

    SetModified(EModificationType::Content);
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
    YT_VERIFY(impl->ChildToIndex().emplace(newTrunkChildImpl, index).second);
    AttachChild(TrunkNode_, newChildImpl);
    objectManager->RefObject(newChildImpl->GetTrunkNode());

    SetModified(EModificationType::Content);
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
    const IYPathServiceContextPtr& context)
{
    return TListNodeMixin::ResolveRecursive(path, context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
