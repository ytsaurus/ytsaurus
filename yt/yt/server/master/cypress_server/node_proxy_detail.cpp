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

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/yson_intern_registry.h>

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

#include <yt/yt/server/lib/sequoia/helpers.h>

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

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/collection_helpers.h>

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
using namespace NSequoiaServer;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

constinit const auto Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

bool IsAccessLoggedMethod(const std::string& method)
{
    static const THashSet<std::string> methodsForAccessLog = {
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
        "LockCopyDestination",
        "LockCopySource",
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

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::TCustomAttributeDictionary(
    TNontemplateCypressNodeProxyBase* proxy)
    : Proxy_(proxy)
{ }

auto TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::ListKeys() const -> std::vector<TKey>
{
    auto keys = ListNodeAttributes(
        Proxy_->Bootstrap_->GetCypressManager(),
        Proxy_->TrunkNode_,
        Proxy_->Transaction_);
    return {keys.begin(), keys.end()};
}

auto TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::ListPairs() const -> std::vector<TKeyValuePair>
{
    auto pairs = GetNodeAttributes(
        Proxy_->Bootstrap_->GetCypressManager(),
        Proxy_->TrunkNode_,
        Proxy_->Transaction_);
    return std::vector<TKeyValuePair>(pairs.begin(), pairs.end());
}

auto TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::FindYson(TKeyView name) const -> TValue
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

void TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::SetYson(TKeyView key, const TYsonString& value)
{
    YT_ASSERT(value);

    Proxy_->GuardedValidateCustomAttributeUpdate(key, value);

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

bool TNontemplateCypressNodeProxyBase::TCustomAttributeDictionary::Remove(TKeyView key)
{
    if (!FindYson(key)) {
        return false;
    }

    Proxy_->GuardedValidateCustomAttributeRemoval(key);

    const auto& cypressManager = Proxy_->Bootstrap_->GetCypressManager();
    auto* node = cypressManager->LockNode(
        Proxy_->TrunkNode_,
        Proxy_->Transaction_,
        TLockRequest::MakeSharedAttribute(key));

    auto* userAttributes = node->GetMutableAttributes();
    if (node->GetTransaction()) {
        userAttributes->Set(key, {});
    } else {
        YT_VERIFY(userAttributes->TryRemove(key));
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
    impl->SetParent(parent ? ICypressNodeProxy::FromNode(parent.Get())->GetTrunkNode()->As<TCompositeCypressNode>() : nullptr);
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
            if (GetThisImpl()->IsSequoia()) {
                THROW_ERROR_EXCEPTION("Attribute %Qv cannot be read directly from master server",
                    EInternedAttributeKey::RecursiveResourceUsage.Unintern());
            }
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            return cypressManager->ComputeRecursiveResourceUsage(GetTrunkNode(), GetTransaction());
        }

        case EInternedAttributeKey::WrongDoorAsync: {
            if (!Bootstrap_->GetConfig()->ExposeTestingFacilities) {
                break;
            }

            THROW_ERROR_EXCEPTION("Error reading the attribute")
                << TErrorAttribute("attribute_key", EInternedAttributeKey::WrongDoorAsync);
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
    SetAllowResolveFromSequoiaObject(&req->Header(), true);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto channel = multicellManager->GetMasterChannelOrThrow(externalCellTag, NHydra::EPeerKind::Follower);
    auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(channel));

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto batchReq = proxy.ExecuteBatch();
    SetAuthenticationIdentity(batchReq, NRpc::TAuthenticationIdentity(user->GetName()));
    batchReq->AddRequest(std::move(req));
    return batchReq->Invoke().Apply(BIND([=, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
        auto error = GetCumulativeError(batchRspOrError);
        if (!error.IsOK()) {
            auto code = error.GetCode();
            if (code == NYTree::EErrorCode::ResolveError || code == NTransactionClient::EErrorCode::NoSuchTransaction) {
                return TYsonString();
            }
            THROW_ERROR_EXCEPTION(code, "Error requesting attribute %Qv of object %v from cell %v",
                key,
                GetVersionedId(),
                externalCellTag)
                << error;
        }

        const auto& batchRsp = batchRspOrError.Value();
        const auto& rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
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

            auto name = ConvertTo<std::string>(value);
            auto* account = securityManager->GetAccountByNameOrThrow(name, /*activeLifeStageOnly*/ true);

            ValidateStorageParametersUpdate();
            ValidatePermission(account, EPermission::Use);

            auto* node = LockThisImpl();
            if (node->Account() != account) {
                // TODO(savrus): See YT-7050.
                securityManager->ValidateResourceUsageIncrease(account, TClusterResources().SetNodeCount(1));
                securityManager->SetAccount(node, account, /*transaction*/ nullptr);
            }

            SetModified(EModificationType::Attributes);

            return true;
        }

        case EInternedAttributeKey::ExpirationTime: {
            ValidateAdHocPermission(EPermission::Remove);

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
            ValidateAdHocPermission(EPermission::Remove);

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

        case EInternedAttributeKey::ExpirationTimeUser: {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto user = ConvertTo<std::string>(value);
            if (securityManager->GetAuthenticatedUser() != securityManager->GetRootUser() || !user.empty()) {
                THROW_ERROR_EXCEPTION(R"("expiration_time_user" is not writable)");
            }

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTime(node, TSetExpirationResetTime{});

            return true;
        }

        case EInternedAttributeKey::ExpirationTimeoutUser: {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto user = ConvertTo<std::string>(value);
            if (securityManager->GetAuthenticatedUser() != securityManager->GetRootUser() || !user.empty()) {
                THROW_ERROR_EXCEPTION(R"("expiration_timeout_user" is not writable)");
            }

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTimeout(node, TSetExpirationResetTime{});

            return true;
        }

        case EInternedAttributeKey::Opaque: {
            ValidateNoTransaction();
            ValidateAdHocPermission(EPermission::Write);

            // NB: No locking, intentionally.
            auto* node = GetThisImpl();
            auto opaque = ConvertTo<bool>(value);
            node->SetOpaque(opaque);

            return true;
        }

        case EInternedAttributeKey::Annotation: {
            auto annotation = ConvertTo<std::optional<std::string>>(value);
            if (annotation) {
                ValidateAnnotation(*annotation);
            }
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedNode = LockThisImpl(lockRequest);
            if (annotation) {
                lockedNode->SetAnnotation(TString(std::move(*annotation)));
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
            cypressManager->SetExpirationTime(node, TRemoveExpiration{});

            return true;
        }

        case EInternedAttributeKey::ExpirationTimeout: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* node = LockThisImpl(lockRequest);
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            cypressManager->SetExpirationTimeout(node, TRemoveExpiration{});

            return true;
        }

        case EInternedAttributeKey::Opaque: {
            ValidateNoTransaction();
            ValidateAdHocPermission(EPermission::Write);

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

    const auto* impl = GetThisImpl();
    // TODO(h0pless): this is not quite correct since multiple changes may get
    // encapsulated into a single Hive mutation.
    if (impl->GetRevision() != NHydra::GetCurrentHydraContext()->GetVersion().ToRevision()) {
        NSecurityServer::LogAcdUpdate(key.Unintern(), GetPath(), value);
    }
}

TVersionedObjectId TNontemplateCypressNodeProxyBase::GetVersionedId() const
{
    return VersionedId_;
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
        .SetPresent(node->GetExpirationTime().has_value())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTimeout)
        .SetPresent(node->GetExpirationTimeout().has_value())
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EffectiveExpiration)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::CreationTime);
    descriptors->push_back(EInternedAttributeKey::ModificationTime);
    descriptors->push_back(EInternedAttributeKey::AccessTime);
    descriptors->push_back(EInternedAttributeKey::AccessCounter);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::NativeContentRevision)
        .SetExternal(true)
        .SetOpaque(true)
        .SetPresent(isExternal));
    descriptors->push_back(EInternedAttributeKey::ResourceUsage);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveResourceUsage)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Account)
        .SetWritable(true)
        .SetReplicated(true));
    descriptors->push_back(EInternedAttributeKey::AccountId);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Opaque)
        .SetWritable(true)
        .SetRemovable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Reachable));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ShardId)
        .SetPresent(node->GetTrunkNode()->GetShard() != nullptr));
    descriptors->push_back(EInternedAttributeKey::ResolveCached);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Annotation)
        .SetWritable(true)
        .SetRemovable(true)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AnnotationPath)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ImmediateAnnotation)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TouchTime)
        .SetPresent(node && node->IsTrunk() && node->GetTouchTime())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTimeUser)
        .SetPresent(node->GetExpirationTimeUser().value_or(nullptr) != nullptr)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTimeoutUser)
        .SetPresent(node->GetExpirationTimeoutUser().value_or(nullptr) != nullptr)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTimeLastResetTime)
        .SetPresent(node->GetExpirationTimeLastResetTime().has_value()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTimeoutLastResetTime)
        .SetPresent(node->GetExpirationTimeoutLastResetTime().has_value()));

    if (Bootstrap_->GetConfig()->ExposeTestingFacilities) {
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WrongDoorSync)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WrongDoorAsync)
            .SetOpaque(true));
    }
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
            static const std::string NullKey("?");
            auto optionalKey = FindNodeKey(
                Bootstrap_->GetCypressManager(),
                GetThisImpl()->GetTrunkNode(),
                Transaction_);
            BuildYsonFluently(consumer)
                .Value(optionalKey.value_or(NullKey));
            return true;
        }

        case EInternedAttributeKey::ExpirationTime: {
            auto optionalExpirationTime = node->GetExpirationTime();
            if (!optionalExpirationTime) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*optionalExpirationTime);
            return true;
        }

        case EInternedAttributeKey::ExpirationTimeout: {
            auto optionalExpirationTimeout = node->GetExpirationTimeout();
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
                            .Item("value").Value(*effectiveNode->GetExpirationTime())
                            .Item("path").Value(cypressManager->GetNodePath(effectiveNode->GetTrunkNode(), GetTransaction()));
                    });
            } else {
                fluent.Item("time").Entity();
            }

            if (auto* effectiveNode = node->GetEffectiveExpirationTimeoutNode()) {
                fluent.Item("timeout")
                    .DoMap([&] (NYTree::TFluentMap fluent) {
                        fluent
                            .Item("value").Value(*effectiveNode->GetExpirationTimeout())
                            .Item("path").Value(cypressManager->GetNodePath(effectiveNode->GetTrunkNode(), GetTransaction()));
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

        case EInternedAttributeKey::AccountId:
            BuildYsonFluently(consumer)
                .Value(node->Account()->GetId());
            return true;

        case EInternedAttributeKey::Opaque:
            BuildYsonFluently(consumer)
                .Value(node->GetOpaque());
            return true;

        case EInternedAttributeKey::Reachable:
            BuildYsonFluently(consumer)
                .Value(node->GetReachable());
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

        case EInternedAttributeKey::ImmediateAnnotation: {
            if (auto annotation = node->TryGetAnnotation()) {
                BuildYsonFluently(consumer)
                    .Value(*annotation);
            } else {
                BuildYsonFluently(consumer)
                    .Entity();
            }
            return true;
        }

        case EInternedAttributeKey::TouchTime:
            if (!node->GetTouchTime()) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(node->GetTouchTime());
            return true;

        case EInternedAttributeKey::ExpirationTimeUser : {
            auto optionalExpirationTimeUser = node->GetExpirationTimeUser();
            if (!optionalExpirationTimeUser) {
                break;
            }

            const auto& expirationTimeUser = *optionalExpirationTimeUser;
            if (!expirationTimeUser) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(expirationTimeUser->GetName());
            return true;
        }

        case EInternedAttributeKey::ExpirationTimeoutUser: {
            auto optionalExpirationTimeoutUser = node->GetExpirationTimeoutUser();
            if (!optionalExpirationTimeoutUser) {
                break;
            }

            const auto& expirationTimeoutUser = *optionalExpirationTimeoutUser;
            if (!expirationTimeoutUser) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(expirationTimeoutUser->GetName());
            return true;
        }

        case EInternedAttributeKey::ExpirationTimeLastResetTime: {
            auto optionalLastReset = node->GetExpirationTimeLastResetTime();
            if (!optionalLastReset) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*optionalLastReset);
            return true;
        }

        case EInternedAttributeKey::ExpirationTimeoutLastResetTime: {
            auto optionalLastReset = node->GetExpirationTimeoutLastResetTime();
            if (!optionalLastReset) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*optionalLastReset);
            return true;
        }

        case EInternedAttributeKey::WrongDoorSync: {
            if (!Bootstrap_->GetConfig()->ExposeTestingFacilities) {
                break;
            }

            THROW_ERROR_EXCEPTION("Error reading the attribute")
                << TErrorAttribute("attribute_key", EInternedAttributeKey::WrongDoorSync);
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

void TNontemplateCypressNodeProxyBase::ValidateMethodWhitelistedForTransaction(const std::string& method) const
{
    const auto& transactionManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->TransactionManager;
    if (!Transaction_ || !transactionManagerConfig->CheckTransactionIsCompatibleWithMethod) {
        return;
    }

    const auto& typeToWhitelist = transactionManagerConfig->TransactionTypeToMethodWhitelist;
    auto transactionType = TypeFromId(Transaction_->GetId());
    auto it = typeToWhitelist.find(transactionType);
    if (it != typeToWhitelist.end() && !it->second.contains(method)) {
        YT_LOG_ALERT_IF(
            transactionManagerConfig->AlertTransactionIsNotCompatibleWithMethod,
            "Attempted to call a method not supported by type "
            "(Method: %v, Type: %v, TransactionId: %v)",
            method,
            transactionType,
            TypeFromId(Transaction_->GetId()));

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
    SequoiaNodeEffectiveAcl_ = GetSequoiaNodeEffectiveAcl(context->RequestHeader());
    ValidateMethodWhitelistedForTransaction(context->GetMethod());

    TObjectProxyBase::BeforeInvoke(context);
}

void TNontemplateCypressNodeProxyBase::AfterInvoke(const IYPathServiceContextPtr& context)
{
    SetAccessed();
    SetTouched();
    SequoiaNodeEffectiveAcl_.reset();
    SequoiaNodeDeserializedEffectiveAcl_.reset();
    TObjectProxyBase::AfterInvoke(context);
}

bool TNontemplateCypressNodeProxyBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    ValidateAccessTransaction();

    auto doInvoke = [&] (const IYPathServiceContextPtr& context) {
        DISPATCH_YPATH_SERVICE_METHOD(Lock);
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(Copy);
        DISPATCH_YPATH_SERVICE_METHOD(LockCopyDestination);
        DISPATCH_YPATH_SERVICE_METHOD(LockCopySource);
        DISPATCH_YPATH_SERVICE_METHOD(SerializeNode);
        DISPATCH_YPATH_SERVICE_METHOD(CalculateInheritedAttributes);
        DISPATCH_YPATH_SERVICE_METHOD(AssembleTreeCopy);
        DISPATCH_YPATH_SERVICE_METHOD(Unlock);
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermission);

        // COMPAT(h0pless): IntroduceNewPipelineForCrossCellCopy.
        DISPATCH_YPATH_SERVICE_METHOD(BeginCopy);
        DISPATCH_YPATH_SERVICE_METHOD(EndCopy);

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

            if (trunkChild->GetNodeType() == ENodeType::Map) {
                VisitMap(trunkChild->As<TCypressMapNode>());
            } else {
                VisitOther(trunkChild);
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

        void VisitMap(TCypressNode* node)
        {
            Writer_.OnBeginMap();
            TKeyToCypressNode keyToChildMapStorage;
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
        MakeShrunkFormattableView(
            attributeFilter,
            GetDynamicCypressManagerConfig()->MaxAttributeFilterSizeToLog),
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
            response->set_value(ToProto(resultOrError.Value()));
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TNontemplateCypressNodeProxyBase::RemoveSelf(
    TReqRemove* request,
    TRspRemove* response,
    const TCtxRemovePtr& context)
{
    auto nodeId = TrunkNode_->GetId();
    auto path = GetPath();

    TNodeBase::RemoveSelf(request, response, context);
    // Node is unreachable after this point.

    YT_LOG_ACCESS_IF(
        GetCausedByNodeExpiration(context->RequestHeader()),
        nodeId,
        path,
        nullptr,
        "TtlRemove");
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

        // Portal exit nodes are reachable by default, but reachability might be manually disabled.
        if (node->GetReachable()) {
            SetUnreachableSubtreeNodes(node);
        }
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
    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    context->SetIncrementalRequestInfo("AttributeFilter: %v",
        MakeShrunkFormattableView(
            attributeFilter,
            GetDynamicCypressManagerConfig()->MaxAttributeFilterSizeToLog));

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

TPermissionCheckResponse TNontemplateCypressNodeProxyBase::DoCheckPermission(
    TUser* user,
    EPermission permission,
    TPermissionCheckOptions options)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    if (Object_->IsSequoia()) {
        if (!SequoiaNodeEffectiveAcl_.has_value()) {
            YT_LOG_ALERT(
                "Missing effective ACL on permission validation for Sequoia node (NodeId: %v)",
                TrunkNode_->GetId());
            THROW_ERROR_EXCEPTION(
                "Permission validation through this API is not supported for Sequoia nodes");
        }

        if (!SequoiaNodeDeserializedEffectiveAcl_.has_value()) {
            auto aclNode = ConvertToNode(TYsonStringBuf(*SequoiaNodeEffectiveAcl_));
            // After removed, the Sequoia ACL table still contains the subject
            // until the next GUQM sync.
            auto result = DeserializeAclGatherMissingAndPendingRemovalSubjectsOrThrow(
                aclNode,
                securityManager,
                /*ignoreMissingSubjects*/ true,
                /*ignorePendingRemovalSubjects*/ false);
            SequoiaNodeDeserializedEffectiveAcl_ = std::move(result.Acl);

            YT_LOG_DEBUG_IF(!result.MissingSubjects.empty(),
                "Missing subjects mentioned in effective ACL for Sequoia node "
                "(NodeId: %v, MissingSubjects: %v)",
                TrunkNode_->GetId(),
                result.MissingSubjects);
        }

        return securityManager->CheckPermission(
            user,
            permission,
            *SequoiaNodeDeserializedEffectiveAcl_,
            std::move(options));
    } else {
        return securityManager->CheckPermission(
            Object_,
            user,
            permission,
            std::move(options));
    }
}

void TNontemplateCypressNodeProxyBase::ValidatePermission(
    EPermissionCheckScope scope,
    EPermission permission,
    const std::string& /*user*/)
{
    auto* node = GetThisImpl();
    if (node->IsSequoia()) {
        // TODO(danilalexeev): YT-24575. Change to alert + throw.
        return;
    }

    // NB: Suppress permission checks for nodes upon construction.
    // Cf. YT-1191, YT-4628.
    auto* trunkNode = node->GetTrunkNode();
    auto shard = trunkNode->GetShard();
    if (trunkNode->GetParent() ||
        shard && trunkNode == shard->GetRoot())
    {
        ValidatePermission(node, scope, permission);
    }
}

void TNontemplateCypressNodeProxyBase::ValidateAdHocPermission(
    EPermission permission,
    const std::string& /*user*/)
{
    auto* node = GetThisImpl();
    if (!node->IsSequoia()) {
        ValidatePermission(EPermissionCheckScope::This, permission);
        return;
    }

    // Detailed permission validation is ignored on Sequoia nodes creation.
    // Cf. TNontemplateCypressNodeProxyBase::ValidatePermission. See YT-26140.
    if (!node->ImmutableSequoiaProperties()) {
        return;
    }

    // TODO(danilalexeev): YT-24575. Remove this check.
    if (!SequoiaNodeEffectiveAcl_.has_value()) {
        return;
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();
    auto checkResponse = DoCheckPermission(user, permission);
    if (checkResponse.Action == ESecurityAction::Deny) {
        securityManager->LogAndThrowAuthorizationError(
            TPermissionCheckTarget{.ObjectId = Object_->GetId()},
            user,
            permission,
            checkResponse);
    }
}

TCompactVector<TObject*, 1> TNontemplateCypressNodeProxyBase::ListDescendantsForPermissionValidation(TCypressNode* node)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* trunkNode = node->GetTrunkNode();
    auto descendants = cypressManager->ListSubtreeNodes(trunkNode, Transaction_, false);

    return TCompactVector<TObject*, 1>(
        descendants.begin(),
        descendants.end());
}

TObject* TNontemplateCypressNodeProxyBase::GetParentForPermissionValidation(TCypressNode* node)
{
    return node->GetParent();
}

void TNontemplateCypressNodeProxyBase::SetReachableSubtreeNodes(TCypressNode* node)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* trunkNode = node->GetTrunkNode();
    cypressManager->SetReachableSubtreeNodes(trunkNode, Transaction_);
}

void TNontemplateCypressNodeProxyBase::SetUnreachableSubtreeNodes(TCypressNode* node)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* trunkNode = node->GetTrunkNode();
    cypressManager->SetUnreachableSubtreeNodes(trunkNode, Transaction_);
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
    TMedium& newPrimaryMedium,
    std::optional<int> oldPrimaryMediumIndex,
    TChunkReplication* newReplication,
    const std::optional<TChunkReplication>& oldReplication)
{
    YT_VERIFY(newReplication);

    ValidateNoTransaction();

    auto tweakReplicationOnPrimaryMediumChange = [] (
        int newPrimaryMediumIndex,
        std::optional<int> oldPrimaryMediumIndex,
        const TChunkReplication& oldReplication)
    {
        auto result = oldReplication;
        if (!result.Get(newPrimaryMediumIndex)) {
            // The user is trying to set a medium with zero replication factor as primary.
            //
            // If primary medium *has* already been set, this is regarded as a
            // request to move from one medium to another.
            //
            // If primary medium *has not* been set previously (which happens for
            // composite nodes and hunk media on chunk owner nodes), this is treated
            // as a request to use default settings (namely, replication factor)
            // when making the medium primary.

            if (oldPrimaryMediumIndex) {
                result.Set(newPrimaryMediumIndex, result.Get(*oldPrimaryMediumIndex));
                result.Erase(*oldPrimaryMediumIndex);
            } else {
                result.Set(newPrimaryMediumIndex, TReplicationPolicy(DefaultReplicationFactor, /*dataPartsOnly*/ false));
            }
        }

        return result;
    };

    if (newPrimaryMedium.GetIndex() == oldPrimaryMediumIndex) {
        return false;
    }

    ValidatePermission(&newPrimaryMedium, EPermission::Use);

    if (!oldReplication) {
        return true;
    }

    auto tweakedReplication = tweakReplicationOnPrimaryMediumChange(
        newPrimaryMedium.GetIndex(),
        oldPrimaryMediumIndex,
        *oldReplication);

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    ValidateChunkReplication(chunkManager, tweakedReplication, newPrimaryMedium.GetIndex());

    *newReplication = std::move(tweakedReplication);
    return true;
}

void TNontemplateCypressNodeProxyBase::SetModified(EModificationType modificationType)
{
    if (ModificationTrackingSuppressed_.test(std::memory_order::relaxed)) {
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

    // NB: Not calling base class method here.
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

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, CheckPermission)
{
    TObjectProxyBase::HandleCheckPermissionRequest(
        Bootstrap_,
        context,
        [&] (TUser* user, EPermission permission, TPermissionCheckOptions options) {
            return DoCheckPermission(user, permission, std::move(options));
        });
}

void TNontemplateCypressNodeProxyBase::Lock(const TLockRequest& request, bool waitable, TLockId lockIdHint)
{
    DoLock(request, waitable, lockIdHint);
}

TNontemplateCypressNodeProxyBase::TLockResult TNontemplateCypressNodeProxyBase::DoLock(
    const TLockRequest& request,
    bool waitable,
    TLockId lockIdHint)
{
    ValidateTransaction();
    ValidatePermission(
        EPermissionCheckScope::This,
        request.Mode == ELockMode::Snapshot ? EPermission::Read : EPermission::Write);
    ValidateLockPossible();

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto lockResult = cypressManager->CreateLock(
        TrunkNode_,
        Transaction_,
        request,
        waitable,
        lockIdHint);

    auto externalTransactionId = Transaction_->GetId();
    if (TrunkNode_->IsExternal()) {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        externalTransactionId = transactionManager->ExternalizeTransaction(
            Transaction_,
            {TrunkNode_->GetExternalCellTag()});
    }

    return {std::move(lockResult), externalTransactionId};
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Lock)
{
    DeclareMutating();

    auto mode = FromProto<ELockMode>(request->mode());
    auto childKey = YT_OPTIONAL_FROM_PROTO(*request, child_key);
    auto attributeKey = YT_OPTIONAL_FROM_PROTO(*request, attribute_key);
    auto timestamp = request->timestamp();
    bool waitable = request->waitable();

    CheckLockRequest(mode, childKey, attributeKey)
        .ThrowOnError();

    auto lockRequest = CreateLockRequest(mode, childKey, attributeKey, timestamp);

    context->SetRequestInfo("Mode: %v, Key: %v, Waitable: %v",
        mode,
        lockRequest.Key,
        waitable);

    auto [lockResult, externalTransactionId] = DoLock(lockRequest, waitable, /*hintLockId*/ {});

    auto externalCellTag = TrunkNode_->IsExternal()
        ? TrunkNode_->GetExternalCellTag()
        : Bootstrap_->GetCellTag();

    auto lockId = lockResult.Lock->GetId();
    auto revision = lockResult.BranchedNode ? lockResult.BranchedNode->GetRevision() : NHydra::NullRevision;

    ToProto(response->mutable_lock_id(), lockId);
    ToProto(response->mutable_node_id(), TrunkNode_->GetId());
    ToProto(response->mutable_external_transaction_id(), externalTransactionId);
    response->set_external_cell_tag(ToProto(externalCellTag));
    response->set_revision(ToProto(revision));

    context->SetResponseInfo("LockId: %v, ExternalCellTag: %v, ExternalTransactionId: %v, Revision: %x",
        lockId,
        externalCellTag,
        externalTransactionId,
        revision);

    context->Reply();
}

void TNontemplateCypressNodeProxyBase::Unlock()
{
    ValidateTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    cypressManager->UnlockNode(TrunkNode_, Transaction_);
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, Unlock)
{
    DeclareMutating();

    context->SetRequestInfo();

    Unlock();

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
    IAttributeDictionaryPtr explicitAttributes;
    if (request->has_node_attributes()) {
        explicitAttributes = FromProto(request->node_attributes());
    }

    context->SetRequestInfo(
        "Type: %v, IgnoreExisting: %v, LockExisting: %v, Recursive: %v, "
        "Force: %v, IgnoreTypeMismatch: %v, HintId: %v, ExplicitAttributeCount: %v, ExplicitInternedAttributeKeys: %v",
        type,
        ignoreExisting,
        lockExisting,
        recursive,
        force,
        ignoreTypeMismatch,
        hintId,
        request->node_attributes().attributes_size(),
        [&] {
            std::vector<std::string> explicitInternedAttributeKeys;
            if (explicitAttributes) {
                explicitInternedAttributeKeys = explicitAttributes->ListKeys();
                std::erase_if(explicitInternedAttributeKeys, [] (const std::string& key) {
                    return TInternedAttributeKey::Lookup(key) == InvalidInternedAttribute;
                });
            }
            return explicitInternedAttributeKeys;
        }());

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
            YT_LOG_ALERT_IF(
                transactionManagerConfig->AlertTransactionIsNotCompatibleWithMethod,
                "Attempted to create an object of type not supported by type "
                "(ObjectType: %v, Type: %v, TransactionId: %v)",
                type,
                TypeFromId(transactionId),
                transactionId);
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
        response->set_cell_tag(ToProto(impl->GetExternalCellTag() == NotReplicatedCellTagSentinel
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

    auto* node = GetThisImpl();

    // The node inside which the new node must be created.
    TCypressNode* intendedParentNode = node;
    if (replace) {
        if (!node->GetParent()) {
            ThrowCannotReplaceNode(this);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        intendedParentNode = cypressManager->GetVersionedNode(node->GetParent(), Transaction_);
    }

    ValidateCreatePermissions(node, replace, explicitAttributes.Get());

    auto* account = intendedParentNode->Account().Get();

    auto inheritedAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
    GatherInheritableAttributes(
        intendedParentNode,
        &inheritedAttributes->MutableAttributes());

    std::optional<TYPath> optionalTargetPath;
    if (explicitAttributes) {
        optionalTargetPath = explicitAttributes->Find<TYPath>(EInternedAttributeKey::TargetPath.Unintern());

        auto optionalAccount = explicitAttributes->FindAndRemove<std::string>(EInternedAttributeKey::Account.Unintern());
        if (optionalAccount) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            account = securityManager->GetAccountByNameOrThrow(*optionalAccount, /*activeLifeStageOnly*/ true);
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
        auto* parentTrunkNode = intendedParentNode->GetTrunkNode();
        GetProxy(parentTrunkNode)->AsComposite()->ReplaceChild(this, newProxy);
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
    response->set_cell_tag(ToProto(newNodeCellTag));

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
    const auto& sourcePath = ypathExt.additional_paths(0);

    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto mode = FromProto<ENodeCloneMode>(request->mode());

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
    // TODO(cherepashka): fix this.
    auto sourceProxy = cypressManager->TryResolvePathToNodeProxy(sourcePath, /*method*/ "", /*service*/ "", Transaction_);
    if (!sourceProxy) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::CrossCellAdditionalPath,
            "Additional path %v turned out to be cross-cell relative to target path %v that resolves to cell %v",
            ypathExt.original_additional_paths(0),
            ypathExt.original_target_path(),
            Bootstrap_->GetMulticellManager()->GetCellTag());
    }

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

    // The paths may be invalidated by removal below; save them.
    auto loggedSourcePath = YT_EVALUATE_FOR_ACCESS_LOG(sourceProxy->GetPath());
    auto thisNodePath = YT_EVALUATE_FOR_ACCESS_LOG(GetPath());

    TNodeId clonedTrunkNodeId;
    CopyCore(
        context,
        [&] (ICypressNodeFactory* factory, IAttributeDictionary* inheritedAttributes) {
            auto* clonedNode = factory->CloneNode(sourceNode, mode, inheritedAttributes);
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
        loggedSourcePath,
        Transaction_,
        {{"destination_id", ToString(clonedTrunkNodeId)},
         {"destination_path", *thisNodePath}},
        mode == ENodeCloneMode::Move ? "Move" : "Copy");

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, LockCopyDestination)
{
    DeclareMutating();
    ValidateTransaction();

    auto force = request->force();
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto preserveAcl = request->preserve_acl();
    auto recursive = request->recursive();

    auto inplace = request->inplace();
    const auto& targetPath = GetRequestTargetYPath(context->RequestHeader());
    bool replace = targetPath.empty();

    context->SetRequestInfo(
        "Force: %v, IgnoreExisting: %v, LockExisting: %v, Replace: %v, "
        "Inplace: %v, PreserveAcl: %v, Recursive: %v, TransactionId: %v",
        force,
        ignoreExisting,
        lockExisting,
        replace,
        inplace,
        preserveAcl,
        recursive,
        Transaction_->GetId());

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (replace && !force && !inplace) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(this);
        }

        if (lockExisting) {
            LockThisImpl();
        }

        ToProto(response->mutable_existing_node_id(), TrunkNode_->GetId());
        context->SetResponseInfo("ExistingNodeId: %v",
            TrunkNode_->GetId());
        context->Reply();
        return;
    }

    if (!recursive && !replace) {
        // TODO(h0pless): Use TYPath from Sequoia client when it'll be available to use in master.
        NYPath::TTokenizer tokenizer(targetPath);
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto childNodeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();
        if (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
            ThrowNoSuchChildKey(this, childNodeKey);
        }
    }

    if (!replace && !CanHaveChildren()) {
        ThrowCannotHaveChildren(this);
    }

    auto* node = GetThisImpl();

    std::string childNodeKey;
    // The node inside which the cloned node must be created. Usually it's the current one.
    auto* parentNode = node;
    if (!inplace) {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        if (replace) {
            if (!node->GetParent()) {
                ThrowCannotReplaceNode(this);
            }

            // COMPAT(h0pless)
            if (GetDynamicCypressManagerConfig()->UseProperBranchedParentInLockCopyDestination) {
                parentNode = cypressManager->GetVersionedNode(node->GetParent(), Transaction_);
            } else {
                parentNode = node->GetParent();
            }

            childNodeKey = FindMapNodeChildKey(parentNode->As<TCypressMapNode>(), node->GetTrunkNode());
        } else {
            // TODO(h0pless): Use TYPath from Sequoia client when it'll be available to use in master.
            NYPath::TTokenizer tokenizer(targetPath);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            childNodeKey = tokenizer.GetLiteralValue();
        }

        // This lock ensures that both parent node and child node won't change before AssembleTreeCopy is called.
        // For inplace this is not needed, since the node is freshly created.
        cypressManager->LockNode(
            parentNode->GetTrunkNode(),
            Transaction_,
            TLockRequest::MakeSharedChild(childNodeKey));
    }

    ValidateCopyToThisDestinationPermissions(node, replace && !inplace, preserveAcl);

    auto* account = parentNode->GetTrunkNode()->Account().Get();

    auto effectiveInheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
    if (GetDynamicCypressManagerConfig()->EnableInheritAttributesDuringCopy && !inplace) {
        YT_LOG_ALERT_AND_THROW_UNLESS(
            IsCompositeNodeType(parentNode->GetType()),
            "Attempt to copy to a non-composite node was made (ParentNodeId: %v, ParentNodeType: %v)",
            parentNode->GetVersionedId(),
            parentNode->GetType());

        // All attributes CAN be recalculated upon copy now, but this doesn't mean that we HAVE TO.
        GatherInheritableAttributes(
            parentNode,
            &effectiveInheritableAttributes->MutableAttributes(),
            ENodeMaterializationReason::Copy);
    }

    // TODO(h0pless): Maybe create all nodes all the way up to PARENT node?
    // This would allow us to remove this chunk of code from CopyCore, maybe move it to some other function.
    // Unfortunately inplace makes this code less generic than desired.

    auto nativeCellTag = node->GetNativeCellTag().Underlying();
    context->SetResponseInfo("NativeCellTag: %v, AccountId: %v, EffectiveInheritedAttributes: %v",
        nativeCellTag,
        account->GetId(),
        effectiveInheritableAttributes->ListPairs());

    response->set_native_cell_tag(nativeCellTag);
    ToProto(response->mutable_account_id(), account->GetId());
    ToProto(response->mutable_effective_inheritable_attributes(), *effectiveInheritableAttributes);
    response->set_sequoia_destination(false);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, LockCopySource)
{
    DeclareMutating();
    ValidateTransaction();

    auto mode = FromProto<ENodeCloneMode>(request->mode());

    context->SetRequestInfo("Mode: %v, Transaction: %v",
        mode,
        Transaction_->GetId());

    auto* node = GetThisImpl();

    ValidatePermission(node, EPermissionCheckScope::Subtree, EPermission::FullRead);

    auto maxSubtreeSize = GetDynamicCypressManagerConfig()->CrossCellCopyMaxSubtreeSize;

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    i64 subtreeSize = 0;
    // It's important that parent node is saved before its children are.
    std::vector<TCypressNode*> dfsQueue;
    dfsQueue.push_back(node->GetTrunkNode());
    while (!dfsQueue.empty()) {
        auto* currentTrunkNode = dfsQueue.back();
        dfsQueue.pop_back();

        cypressManager->LockNode(
            currentTrunkNode,
            Transaction_,
            mode == ENodeCloneMode::Copy ? ELockMode::Snapshot : ELockMode::Exclusive);

        auto nodeType = currentTrunkNode->GetType();
        if (nodeType == EObjectType::PortalEntrance) {
            THROW_ERROR_EXCEPTION("Portal entrances cannot be copied");
        }

        if (nodeType != EObjectType::MapNode && nodeType != EObjectType::PortalExit) {
            // NB: Other types that can have children are all Sequoia-related and are handled on Cypress proxy.
            continue;
        }

        auto* nodeIdToChildrenEntry = response->add_node_id_to_children();
        ToProto(nodeIdToChildrenEntry->mutable_node_id(), currentTrunkNode->GetId());
        TKeyToCypressNode keyToChildMapStorage;
        const auto& keyToChildMap = GetMapNodeChildMap(
            cypressManager,
            currentTrunkNode->As<TCypressMapNode>(),
            Transaction_,
            &keyToChildMapStorage);

        for (const auto& [key, trunkChild] : SortHashMapByKeys(keyToChildMap)) {
            auto* childEntry = nodeIdToChildrenEntry->add_children();
            childEntry->set_key(key);
            ToProto(childEntry->mutable_id(), trunkChild->GetId());
            dfsQueue.push_back(trunkChild);
        }

        if (++subtreeSize >= maxSubtreeSize) {
            THROW_ERROR_EXCEPTION("Subtree is too large for cross-cell copy")
                << TErrorAttribute("subtree_size", subtreeSize)
                << TErrorAttribute("max_subtree_size", maxSubtreeSize);
        }
    }

    response->set_version(GetCurrentReign());
    ToProto(response->mutable_root_node_id(), node->GetId());

    context->SetResponseInfo("NodeCount: %v",
        response->node_id_to_children_size());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, SerializeNode)
{
    DeclareNonMutating();
    ValidateTransaction();

    auto mode = FromProto<ENodeCloneMode>(request->mode());
    context->SetRequestInfo("NodeId: %v, Mode: %v",
        GetVersionedId(),
        mode);

    auto* node = GetThisImpl();
    ValidatePermission(node, EPermissionCheckScope::This, EPermission::FullRead);

    TSerializeNodeContext nodeLocalContext(Transaction_, mode, node);
    const auto& handler = Bootstrap_->GetCypressManager()->GetHandler(node->GetTrunkNode());
    handler->SerializeNode(node, &nodeLocalContext);

    auto data = nodeLocalContext.Finish();
    auto mergedData = data.size() == 1
        ? data[0]
        : MergeRefsToRef<TDefaultBlobTag>(data);
    auto* resultingEntry = response->mutable_serialized_node();
    resultingEntry->set_data(mergedData.begin(), mergedData.size());

    if (node->IsExternal()) {
        auto cellTag = node->GetExternalCellTag();
        resultingEntry->set_external_cell_tag(ToProto(cellTag));
    }

    if (auto schemaId = nodeLocalContext.GetSchemaId()) {
        ToProto(resultingEntry->mutable_schema_id(), schemaId);
    }

    ToProto(resultingEntry->mutable_node_id(), node->GetId());
    context->SetResponseInfo("SerializedNodeDataSize: %v",
        mergedData.size());
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, CalculateInheritedAttributes)
{
    DeclareNonMutating();
    ValidateTransaction();

    auto dstInheritedAttributes = FromProto(request->dst_attributes());
    auto shouldCalculateInheritedAttributes = GetDynamicCypressManagerConfig()->EnableInheritAttributesDuringCopy;

    context->SetRequestInfo("DestinationInheritedAttributes: %v, ShouldCalculateInheritedAttributes: %v",
        dstInheritedAttributes->ListPairs(),
        shouldCalculateInheritedAttributes);

    if (!shouldCalculateInheritedAttributes) {
        context->Reply();
        return;
    }

    const auto& cypressManager = Bootstrap_->GetCypressManager();

    // NB: This call will always return node under the current transaction, since
    // the node has been locked under current transaction in LockCopySource.
    auto* node = GetThisImpl();

    YT_LOG_ALERT_AND_THROW_UNLESS(node->GetTransaction() == Transaction_,
        "Inconsistent locking during copy detected (NodeId: %v, ExpectedTransactionId: %v)",
        node->GetVersionedId(),
        GetObjectId(Transaction_));

    auto iterateOverAttributeDeltaDuringInheritance = [] (
        const TConstInheritedAttributeDictionaryPtr& inheritedAttributes,
        const THashMap<std::string, NYson::TYsonString>& nodeAttributes,
        auto onDifferentAttribute) {
        for (const auto& [key, value] : inheritedAttributes->ListPairs()) {
            auto nodeAttributeIt = nodeAttributes.find(key);
            if (nodeAttributeIt == nodeAttributes.end() || nodeAttributeIt->second != value) {
                onDifferentAttribute(key, value);
            }
        }
    };

    std::vector<std::pair<TCypressNode*, TConstInheritedAttributeDictionaryPtr>> traverseQueue;
    traverseQueue.emplace_back(
        node,
        New<TInheritedAttributeDictionary>(Bootstrap_, std::move(dstInheritedAttributes)));

    while (!traverseQueue.empty()) {
        auto [currentNode, inheritedAttributes] = traverseQueue.back();
        traverseQueue.pop_back();
        if (!IsCompositeNodeType(currentNode->GetType())) {
            auto currentNodeAttributes = GetNodeAttributes(
                cypressManager,
                currentNode->GetTrunkNode(),
                currentNode->GetTransaction());

            // Sadly, I don't see any way to avoid using decltype here; actual type of proto field is way scarier.
            decltype(response->add_node_to_attribute_deltas()) delta = nullptr;
            iterateOverAttributeDeltaDuringInheritance(
                inheritedAttributes,
                currentNodeAttributes,
                [&] (const std::string& key, const TYsonString& inheritedValue) {
                    if (!delta) {
                        delta = response->add_node_to_attribute_deltas();
                        ToProto(delta->mutable_node_id(), currentNode->GetId());
                    }

                    auto* attributeOverrideDictionary = delta->mutable_attributes();

                    // Adding attributes directly to attribute dictionary proto.
                    // This helps to avoid creating an extra ephemeral attributes instance.
                    auto* attributeOverride = attributeOverrideDictionary->add_attributes();
                    attributeOverride->set_key(key);
                    attributeOverride->set_value(ToProto(inheritedValue));
                });
            continue;
        }

        if (currentNode->GetType() != EObjectType::MapNode && currentNode->GetType() != EObjectType::PortalExit) {
            THROW_ERROR_EXCEPTION("Type %Qlv cannot be cross-cell copied", currentNode->GetType());
        }

        TKeyToCypressNode keyToChildMapStorage;
        const auto& keyToChildMap = GetMapNodeChildMap(
            cypressManager,
            currentNode->GetTrunkNode()->As<TCypressMapNode>(),
            currentNode->GetTransaction(),
            &keyToChildMapStorage);

        auto* currentCompositeNode = currentNode->As<TCompositeCypressNode>();
        auto childInheritedAttributes = currentCompositeNode->MaybePatchInheritableAttributes(
            inheritedAttributes,
            ENodeMaterializationReason::Copy);

        for (const auto& [key, trunkChild] : keyToChildMap) {
            auto* child = cypressManager->GetVersionedNode(trunkChild, node->GetTransaction());
            traverseQueue.emplace_back(child, childInheritedAttributes);
        }
    }

    context->SetResponseInfo("NodeToAttributeDeltasSize: %v",
        response->node_to_attribute_deltas_size());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, AssembleTreeCopy)
{
    DeclareMutating();
    ValidateTransaction();

    // COMPAT(h0pless): IntroduceCypressToSequoiaCopy.
    // Remove this once "AllowBypassMasterResolve" option is used everywhere.
    if (request->sequoia_destination()) {
        THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::RequestInvolvesSequoia,
            "Received direct request to assemble Sequoia tree in Cypress");
    }

    bool inplace = request->inplace();
    // Other fields are logged in CopyCore.
    context->SetIncrementalRequestInfo("Inplace: %v", inplace);

    bool preserveModificationTime = request->preserve_modification_time();
    bool preserveAcl = request->preserve_acl();

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto rootNodeId = FromProto<TNodeId>(request->root_node_id());
    // Sanity checks.
    YT_ASSERT(request->node_id_to_children_size() != 0);
    YT_ASSERT(rootNodeId == FromProto<TNodeId>(request->node_id_to_children()[0].node_id()));
    // This node is needed for access log evaluation down the line.
    auto* rootNode = cypressManager->GetNode({rootNodeId, Transaction_->GetId()});

    auto assembleTreeCopy = [&] (ICypressNodeFactory* /*factory*/, IAttributeDictionary* /*inheritedAttributes*/) {
        auto shard = TrunkNode_->GetShard();
        auto finishAttachingNode = [&] (TCypressNode* node) {
            auto* trunkNode = node->GetTrunkNode();

            // Shard and acl are only present on the trunk version of the node.
            // Shard was unknown during materialization phase.
            cypressManager->SetShard(trunkNode, shard);

            if (!preserveAcl) {
                // Acls are always preserved during materialization.
                trunkNode->Acd().ClearEntries();
                trunkNode->Acd().SetInherit(true);
            }

            if (!preserveModificationTime) {
                node->SetModified(EModificationType::Content);
            }
        };

        // Process first node outside of the main for loop, but only for freshly created nodes.
        if (!inplace) {
            finishAttachingNode(rootNode);
        }

        for (const auto& nodeIdToChild : request->node_id_to_children()) {
            auto nodeId = FromProto<TNodeId>(nodeIdToChild.node_id());
            auto* currentTrunkNode = cypressManager->GetNode({nodeId, NullTransactionId});
            auto modificationTime = currentTrunkNode->GetModificationTime();

            auto currentNodeProxy = GetProxy(currentTrunkNode);
            for (const auto& child : nodeIdToChild.children()) {
                auto childId = FromProto<TNodeId>(child.id());
                auto* childNode = cypressManager->GetNode({childId, Transaction_->GetId()});
                auto* childTrunkNode = childNode->GetTrunkNode();

                currentNodeProxy->SetChildNode(
                    /*factory*/ nullptr,
                    "/" + ToYPathLiteral(child.key()),
                    GetProxy(childTrunkNode),
                    /*recursive*/ false);

                finishAttachingNode(childNode);
            }

            auto* currentBranchNode = cypressManager->GetNode({nodeId, Transaction_->GetId()});
            if (preserveModificationTime) {
                currentBranchNode->SetModificationTime(modificationTime);
                currentTrunkNode->SetModificationTime(modificationTime);
            }
        }

        return cypressManager->GetNodeOrThrow({rootNodeId, Transaction_->GetId()});
    };

    // TODO(h0pless): Maybe we can get rid of "inplace" somehow? Think about it.
    CopyCore(
        context,
        assembleTreeCopy,
        inplace);

    YT_LOG_ACCESS_IF(
        IsAccessLoggedType(rootNode->GetType()),
        context,
        rootNode->GetId(),
        cypressManager->GetNodePath(rootNode->GetTrunkNode(), Transaction_),
        Transaction_);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, BeginCopy)
{
    context->SetRequestInfo("Mode: %v",
        FromProto<ENodeCloneMode>(request->mode()));

    THROW_ERROR_EXCEPTION(
        NObjectClient::EErrorCode::BeginCopyDeprecated,
        "BeginCopy verb is deprecated");
}

DEFINE_YPATH_SERVICE_METHOD(TNontemplateCypressNodeProxyBase, EndCopy)
{
    YT_LOG_ALERT("Received EndCopy request (Version: %v)",
        request->version());

    THROW_ERROR_EXCEPTION("EndCopy verb is deprecated");
}

TNodeFactoryOptions TNontemplateCypressNodeProxyBase::GetFactoryOptionsAndLog(const TCtxAssembleTreeCopyPtr& context) const
{
    auto* request = &context->Request();
    auto preserveModificationTime = request->preserve_modification_time();
    auto pessimisticQuotaCheck = request->pessimistic_quota_check();
    bool preserveAcl = request->preserve_acl();

    context->SetIncrementalRequestInfo(
        "PreserveModificationTime: %v, PreserveAcl: %v, PessimisticQuotaCheck: %v",
        preserveModificationTime,
        preserveAcl,
        pessimisticQuotaCheck);

    return TNodeFactoryOptions{
        .PreserveAccount = /*preserveAccount*/ false,
        .PreserveCreationTime = /*preserveCreationTime*/ false,
        .PreserveModificationTime = preserveModificationTime,
        .PreserveExpirationTime = /*preserveExpirationTime*/ false,
        .PreserveExpirationTimeout = /*preserveExpirationTimeout*/ false,
        .PreserveOwner = /*preserveOwner*/ false,
        .PreserveAcl = preserveAcl,
        .PessimisticQuotaCheck = pessimisticQuotaCheck
    };
}

TNodeFactoryOptions TNontemplateCypressNodeProxyBase::GetFactoryOptionsAndLog(const TCtxCopyPtr& context) const
{
    auto* request = &context->Request();

    auto preserveAccount = request->preserve_account();
    auto preserveCreationTime = request->preserve_creation_time();
    auto preserveModificationTime = request->preserve_modification_time();
    auto preserveExpirationTime = request->preserve_expiration_time();
    auto preserveExpirationTimeout = request->preserve_expiration_timeout();
    auto preserveOwner = request->preserve_owner();
    auto pessimisticQuotaCheck = request->pessimistic_quota_check();
    auto preserveAcl = request->preserve_acl();
    auto allowSecondaryIndexAbandonment = request->allow_secondary_index_abandonment();

    context->SetIncrementalRequestInfo(
        "PreserveAccount: %v, PreserveCreationTime: %v, PreserveModificationTime: %v, PreserveExpirationTime: %v, "
        "PreserveExpirationTimeout: %v, PreserveOwner: %v, PreserveAcl: %v, PessimisticQuotaCheck: %v, "
        "AllowSecondaryIndexAbandonment: %v",
        preserveAccount,
        preserveCreationTime,
        preserveModificationTime,
        preserveExpirationTime,
        preserveExpirationTimeout,
        preserveOwner,
        preserveAcl,
        pessimisticQuotaCheck,
        allowSecondaryIndexAbandonment);

    return TNodeFactoryOptions{
        .PreserveAccount = preserveAccount,
        .PreserveCreationTime = preserveCreationTime,
        .PreserveModificationTime = preserveModificationTime,
        .PreserveExpirationTime = preserveExpirationTime,
        .PreserveExpirationTimeout = preserveExpirationTimeout,
        .PreserveOwner = preserveOwner,
        .PreserveAcl = preserveAcl,
        .PessimisticQuotaCheck = pessimisticQuotaCheck,
        .AllowSecondaryIndexAbandonment = allowSecondaryIndexAbandonment,
    };
}

template <class TContextPtr, class TClonedTreeBuilder>
void TNontemplateCypressNodeProxyBase::CopyCore(
    const TContextPtr& context,
    const TClonedTreeBuilder& clonedTreeBuilder,
    bool inplace)
{
    auto* request = &context->Request();
    auto* response = &context->Response();

    const auto& targetPath = GetRequestTargetYPath(context->RequestHeader());
    bool preserveAcl = request->preserve_acl();
    auto recursive = request->recursive();
    auto ignoreExisting = request->ignore_existing();
    auto lockExisting = request->lock_existing();
    auto force = request->force();

    auto nodeFactoryOptions = GetFactoryOptionsAndLog(context);

    context->SetRequestInfo(
        "TransactionId: %v, Recursive: %v, IgnoreExisting: %v, LockExisting: %v, Force: %v",
        NObjectServer::GetObjectId(Transaction_),
        recursive,
        ignoreExisting,
        lockExisting,
        force);

    if (inplace && TrunkNode_->GetType() != EObjectType::PortalExit) {
        THROW_ERROR_EXCEPTION("Cannot load inplace any node except portal exit")
            << TErrorAttribute("node_id", TrunkNode_->GetId())
            << TErrorAttribute("transaction_id", Transaction_->GetId());
    }

    if (ignoreExisting && force) {
        THROW_ERROR_EXCEPTION("Cannot specify both \"ignore_existing\" and \"force\" options simultaneously");
    }

    if (!ignoreExisting && lockExisting) {
        THROW_ERROR_EXCEPTION("Cannot specify \"lock_existing\" without \"ignore_existing\"");
    }

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

    auto* node = GetThisImpl();

    // The node inside which the cloned node must be created.
    TCypressNode* parentNode;
    if (replace && !inplace) {
        if (!node->GetParent()) {
            ThrowCannotReplaceNode(this);
        }
        parentNode = node->GetParent();
    } else {
        parentNode = node;
    }

    ValidateCopyToThisDestinationPermissions(node, replace && !inplace, preserveAcl);

    auto* account = parentNode->Account().Get();

    auto factory = CreateCypressFactory(
        account,
        nodeFactoryOptions,
        targetPath);

    auto inheritedAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
    if (GetDynamicCypressManagerConfig()->EnableInheritAttributesDuringCopy && !inplace) {
        YT_LOG_ALERT_AND_THROW_UNLESS(
            IsCompositeNodeType(parentNode->GetType()),
            "Attempt to copy to a non-composite node was made (ParentNodeId: %v, ParentNodeType: %v)",
            parentNode->GetVersionedId(),
            parentNode->GetType());

        GatherInheritableAttributes(
            parentNode,
            &inheritedAttributes->MutableAttributes(),
            ENodeMaterializationReason::Copy);
    }

    auto* clonedNode = clonedTreeBuilder(factory.get(), inheritedAttributes.Get());
    auto* clonedTrunkNode = clonedNode->GetTrunkNode();
    if (!inplace) {
        auto clonedProxy = GetProxy(clonedTrunkNode);
        if (replace) {
            auto* trunkParentNode = parentNode->GetTrunkNode();
            // NB: GetProxy returns proxy under current transaction, but requires trunkNode as it's parameter.
            GetProxy(trunkParentNode)->AsComposite()->ReplaceChild(this, clonedProxy);
        } else {
            SetChildNode(
                factory.get(),
                targetPath,
                clonedProxy,
                recursive);
        }
    } else if (clonedTrunkNode->GetReachable()) {
        // TODO(h0pless): I think this is a lie?
        // Due to the MaterializeNode with inplace flag specifics a subtree becomes immediately visible in trunk.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetReachableSubtreeNodes(clonedTrunkNode, /*transaction*/ nullptr, /*includeRoot*/ false);
    } else {
        YT_LOG_WARNING("Copy inplace target node is unreachable (NodeId: %v)",
            clonedTrunkNode->GetVersionedId());
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

    auto useWeakerAccessValidationCheck = GetDynamicCypressManagerConfig()->UseWeakerAccessValidationCheck;
    if ((Transaction_->IsExternalized() || useWeakerAccessValidationCheck) &&
        Object_->GetNativeCellTag() == Transaction_->GetNativeCellTag())
    {
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

    const auto* node = GetThisImpl<TCompositeCypressNode>();

    descriptors->push_back(EInternedAttributeKey::Count);

    auto hasInheritableAttributes = node->HasInheritableAttributes();

    const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ChunkMerger; \

#define INHERITABLE_ATTRIBUTE_DESCRIPTOR(FieldName, AttributeKey) \
    TAttributeDescriptor(EInternedAttributeKey::AttributeKey) \
        .SetPresent(hasInheritableAttributes && node->Has##FieldName()) \
        .SetWritable(true) \
        .SetRemovable(true)

#define XX(FieldName, AttributeKey) \
    descriptors->push_back(INHERITABLE_ATTRIBUTE_DESCRIPTOR(FieldName, AttributeKey));
    FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    auto descriptorCompressionCodec = INHERITABLE_ATTRIBUTE_DESCRIPTOR(CompressionCodec, CompressionCodec);
    auto descriptorErasureCodec = INHERITABLE_ATTRIBUTE_DESCRIPTOR(ErasureCodec, ErasureCodec);
    auto descriptorPrimaryMedium = INHERITABLE_ATTRIBUTE_DESCRIPTOR(PrimaryMediumIndex, PrimaryMedium);
    auto descriptorHunkPrimaryMedium = INHERITABLE_ATTRIBUTE_DESCRIPTOR(HunkPrimaryMediumIndex, HunkPrimaryMedium);
    auto descriptorReplicationFactor = INHERITABLE_ATTRIBUTE_DESCRIPTOR(ReplicationFactor, ReplicationFactor);
    auto descriptorTabletCellBundle = INHERITABLE_ATTRIBUTE_DESCRIPTOR(TabletCellBundle, TabletCellBundle);
    auto descriptorChaosCellBundle = INHERITABLE_ATTRIBUTE_DESCRIPTOR(ChaosCellBundle, ChaosCellBundle);
    auto descriptorMedia = INHERITABLE_ATTRIBUTE_DESCRIPTOR(Media, Media);
    auto descriptorHunkMedia = INHERITABLE_ATTRIBUTE_DESCRIPTOR(HunkMedia, HunkMedia);

    auto descriptorChunkMergerMode = INHERITABLE_ATTRIBUTE_DESCRIPTOR(ChunkMergerMode, ChunkMergerMode);
    if (!config->AllowSettingChunkMergerMode) {
        descriptorChunkMergerMode.SetWritePermission(EPermission::Administer);
    }

#define XX(FieldName, AttributeKey) \
    descriptors->push_back(descriptor##AttributeKey);
    FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(XX)
#undef XX

    // NB: attribute aliases are read-only.
#define XX(FieldName, AttributeKey, AliasAttributeKey) \
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AliasAttributeKey) \
        .SetPresent(hasInheritableAttributes && node->Has##FieldName()));
    FOR_EACH_INHERITABLE_ATTRIBUTE_TRANSFERABLE_ALIAS(XX)
#undef XX

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EffectiveInheritableAttributes)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TransferableEffectiveInheritableAttributes)
        .SetOpaque(true));
}

bool TNontemplateCompositeCypressNodeProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* node = GetThisImpl<TCompositeCypressNode>();

    auto doProduceValue = [&] (const auto& value) {
        BuildYsonFluently(consumer)
            .Value(value);
    };

    auto doProduceObjectId = [&] (const auto* object) {
        YT_ASSERT(object);
        doProduceValue(object->GetId());
    };

    auto doProduceTransferableMedia = [&] (const TChunkReplication& replication) {
        doProduceValue(TSerializableTransferableChunkReplication(replication, Bootstrap_->GetChunkManager()));
    };

    auto doProduceMedia = [&] (const TChunkReplication& replication) {
        doProduceValue(TSerializableChunkReplication(replication, Bootstrap_->GetChunkManager()));
    };

    auto doProduceObjectName = [&] (const auto* object) {
        doProduceValue(object->GetName());
    };

    auto doProduceMediumName = [&] (int mediumIndex) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
        doProduceObjectName(medium);
    };

    auto doProduceMediumId = [&] (int mediumIndex) {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
        doProduceObjectId(medium);
    };

    // Complicated inheritable attributes.
    auto produceCompressionCodec = doProduceValue;
    auto produceErasureCodec = doProduceValue;
    auto produceTabletCellBundle = doProduceObjectName;
    auto produceChaosCellBundle = doProduceObjectName;
    auto produceMedia = doProduceMedia;
    auto produceHunkMedia = doProduceMedia;
    auto producePrimaryMedium = doProduceMediumName;
    auto produceHunkPrimaryMedium = doProduceMediumName;
    auto produceReplicationFactor = doProduceValue;
    auto produceChunkMergerMode = doProduceValue;

    // Inheritable attribute aliases.
    auto produceTabletCellBundleId = doProduceObjectId;
    auto produceChaosCellBundleId = doProduceObjectId;
    auto producePrimaryMediumId = doProduceMediumId;
    auto produceHunkPrimaryMediumId = doProduceMediumId;
    auto produceTransferableMedia = doProduceTransferableMedia;
    auto produceTransferableHunkMedia = doProduceTransferableMedia;

    switch (key) {
    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: { \
            auto value = node->TryGet##FieldName(); \
            if (!value) { \
                break; \
            } \
            doProduceValue(*value); \
            return true; \
        }

        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: { \
            auto value = node->TryGet##FieldName(); \
            if (!value) { \
                break; \
            } \
            produce##AttributeKey(*value); \
            return true; \
        }
        FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

    #define XX(FieldName, AttributeKey, AliasAttributeKey) \
        case EInternedAttributeKey::AliasAttributeKey: { \
            auto value = node->TryGet##FieldName(); \
            if (!value) { \
                break; \
            } \
            produce##AliasAttributeKey(*value); \
            return true; \
        }
        FOR_EACH_INHERITABLE_ATTRIBUTE_TRANSFERABLE_ALIAS(XX)
    #undef XX

        case EInternedAttributeKey::Count:
            BuildYsonFluently(consumer)
                .Value(GetChildCount());
            return true;

        case EInternedAttributeKey::EffectiveInheritableAttributes:
        case EInternedAttributeKey::TransferableEffectiveInheritableAttributes: {
            auto inheritedAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
            GatherInheritableAttributes(
                node->As<TCypressNode>(),
                &inheritedAttributes->MutableAttributes());
            if (key == EInternedAttributeKey::EffectiveInheritableAttributes) {
                inheritedAttributes = inheritedAttributes->ToNonTransferableView();
            }

            BuildYsonFluently(consumer)
                .Value(inheritedAttributes);
            return true;
        }

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
}

bool TNontemplateCompositeCypressNodeProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    // Attributes "media", "primary_medium", "replication_factor" are interrelated
    // and nullable, which greatly complicates their modification.
    //
    // The rule of thumb is: if possible, consistency of non-null attributes is
    // checked, but an attribute is never required to be set just for the
    // purposes of validation of other attributes. For instance: "media" and
    // "replication_factor" are checked for consistency only when "primary_medium" is
    // set. Without it, it's impossible to tell which medium the "replication_factor"
    // pertains to, and these attributes may be modified virtually independently.

    auto* node = GetThisImpl<TCompositeCypressNode>();

    auto doSetPrimaryMedium = [&] (bool isHunk) {
        SetPrimaryMedium(isHunk, ConvertTo<std::string>(value));
    };

    auto doSetMedia = [&] (bool isHunk) {
        SetMedia(isHunk, ConvertTo<TSerializableChunkReplication>(value));
    };

    auto doLockAndSet = [&] (auto setter, auto value) {
        auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
        auto* lockedNode = LockThisImpl<TCompositeCypressNode>(lockRequest);
        (lockedNode->*setter)(value);
    };

    // Complicated inheritable attributes.
    auto setCompressionCodec = [&] {
        const auto& chunkManagerConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
        ValidateCompressionCodec(
            value,
            chunkManagerConfig->ForbiddenCompressionCodecs,
            chunkManagerConfig->ForbiddenCompressionCodecNameToAlias);

        doLockAndSet(&TCompositeCypressNode::SetCompressionCodec, ConvertTo<NCompression::ECodec>(value));
    };

    auto setErasureCodec = [&] {
        ValidateErasureCodec(
            value,
            Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ForbiddenErasureCodecs);
        doLockAndSet(&TCompositeCypressNode::SetErasureCodec, ConvertTo<NErasure::ECodec>(value));
    };

    auto setTabletCellBundle = [&] {
        ValidateNoTransaction();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* newBundle = tabletManager->GetTabletCellBundleByNameOrThrow(ConvertTo<std::string>(value), /*isActiveLifeStageOnly*/ true);
        node->SetTabletCellBundle(TTabletCellBundlePtr(newBundle));
    };

    auto setChaosCellBundle = [&] {
        ValidateNoTransaction();

        const auto& chaosManager = Bootstrap_->GetChaosManager();
        auto* newBundle = chaosManager->GetChaosCellBundleByNameOrThrow(ConvertTo<std::string>(value), /*activeLifeStageOnly*/ true);
        node->SetChaosCellBundle(TChaosCellBundlePtr(newBundle));
    };

    auto setPrimaryMedium = std::bind(doSetPrimaryMedium, /*isHunk*/ false);
    auto setHunkPrimaryMedium = std::bind(doSetPrimaryMedium, /*isHunk*/ true);
    auto setMedia = std::bind(doSetMedia, /*isHunk*/ false);
    auto setHunkMedia = std::bind(doSetMedia, /*isHunk*/ true);

    auto setReplicationFactor = [&] {
        SetReplicationFactor(ConvertTo<int>(value));
    };

    auto setChunkMergerMode = [&] {
        doLockAndSet(&TCompositeCypressNode::SetChunkMergerMode, ConvertTo<EChunkMergerMode>(value));
    };

    switch (key) {
    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            { \
                using TAttr = decltype(std::declval<TCompositeCypressNode::TPersistentAttributes>().FieldName)::TValue; \
                doLockAndSet(&TCompositeCypressNode::Set##FieldName, ConvertTo<TAttr>(value)); \
            } \
            return true;
        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            set##AttributeKey(); \
            return true;
        FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::SetBuiltinAttribute(key, value, force);
}

void TNontemplateCompositeCypressNodeProxyBase::SetReplicationFactor(int replicationFactor)
{
    ValidateNoTransaction();

    auto* node = GetThisImpl<TCompositeCypressNode>();

    if (replicationFactor == node->TryGetReplicationFactor()) {
        return;
    }

    ValidateReplicationFactor(replicationFactor);

    if (auto mediumIndex = node->TryGetPrimaryMediumIndex()) {
        if (auto replication = node->TryGetMedia()) {
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

void TNontemplateCompositeCypressNodeProxyBase::SetPrimaryMedium(
    bool isHunk,
    const std::string& primaryMediumName)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto& newPrimaryMedium = *chunkManager->GetMediumByNameOrThrow(primaryMediumName);
    auto* node = GetThisImpl<TCompositeCypressNode>();

    auto oldPrimaryMediumIndex = isHunk
        ? node->TryGetHunkPrimaryMediumIndex()
        : node->TryGetPrimaryMediumIndex();
    auto oldReplication = isHunk
        ? node->TryGetHunkMedia()
        : node->TryGetMedia();

    TChunkReplication newReplication;
    if (!ValidatePrimaryMediumChange(
        newPrimaryMedium,
        oldPrimaryMediumIndex,
        &newReplication,
        oldReplication))
    {
        return;
    }

    auto newPrimaryMediumIndex = newPrimaryMedium.GetIndex();

    if (!isHunk) { // There's no dedicated replication factor for hunks.
        if (auto replicationFactor = node->TryGetReplicationFactor();
            replicationFactor &&
            *replicationFactor != newReplication.Get(newPrimaryMediumIndex).GetReplicationFactor())
        {
            ThrowReplicationFactorMismatch(newPrimaryMediumIndex);
        }
    }

    if (isHunk) {
        node->SetHunkPrimaryMediumIndex(newPrimaryMediumIndex);
        if (newReplication.GetSize() != 0) {
            node->SetHunkMedia(newReplication);
        }
    } else {
        node->SetPrimaryMediumIndex(newPrimaryMediumIndex);
        if (newReplication.GetSize() != 0) {
            node->SetMedia(newReplication);
        }
    }
}

void TNontemplateCompositeCypressNodeProxyBase::RemovePrimaryMedium(bool isHunk)
{
    ValidateNoTransaction();

    auto* node = GetThisImpl<TCompositeCypressNode>();

    if (isHunk) {
        node->RemoveHunkPrimaryMediumIndex();
    } else {
        node->RemovePrimaryMediumIndex();
    }
}

void TNontemplateCompositeCypressNodeProxyBase::SetMedia(
    bool isHunk,
    const TSerializableChunkReplication& serializableReplication)
{
    ValidateNoTransaction();

    auto* node = GetThisImpl<TCompositeCypressNode>();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    TChunkReplication newReplication;
    // Vitality isn't a part of TSerializableChunkReplication, assume true.
    newReplication.SetVital(true);
    serializableReplication.ToChunkReplication(&newReplication, chunkManager);

    auto primaryMediumIndex = isHunk
        ? node->TryGetHunkPrimaryMediumIndex()
        : node->TryGetPrimaryMediumIndex();
    auto oldReplication = isHunk
        ? node->TryGetHunkMedia()
        : node->TryGetMedia();

    if (newReplication == oldReplication) {
        return;
    }

    if (!isHunk) { // There's no dedicated replication factor for hunks.
        if (auto replicationFactor = node->TryGetReplicationFactor();
            primaryMediumIndex &&
            replicationFactor &&
            newReplication.Get(*primaryMediumIndex).GetReplicationFactor() != *replicationFactor)
        {
            ThrowReplicationFactorMismatch(*primaryMediumIndex);
        }
    }

    // NB: primary medium index may be null, in which case corresponding
    // parts of validation will be skipped.
    ValidateMediaChange(oldReplication, primaryMediumIndex, newReplication);

    if (isHunk) {
        node->SetHunkMedia(newReplication);
    } else {
        node->SetMedia(newReplication);
    }
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
    auto* node = GetThisImpl<TCompositeCypressNode>();

    auto doLockAndRemove = [&] (auto remover) {
        auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
        auto* lockedNode = LockThisImpl<TCompositeCypressNode>(lockRequest);
        (lockedNode->*remover)();
    };

    auto doNotLockAndRemove = [&] (auto remover) {
        ValidateNoTransaction();
        (node->*remover)();
    };

    auto doRemovePrimaryMedium = [&] (bool isHunk) {
        RemovePrimaryMedium(isHunk);
    };

    // Complicated inheritable attributes.
    auto removeCompressionCodec = std::bind(doLockAndRemove, &TCompositeCypressNode::RemoveCompressionCodec);
    auto removeErasureCodec = std::bind(doLockAndRemove, &TCompositeCypressNode::RemoveErasureCodec);
    auto removeReplicationFactor = std::bind(doLockAndRemove, &TCompositeCypressNode::RemoveReplicationFactor);
    auto removeChunkMergerMode = std::bind(doLockAndRemove, &TCompositeCypressNode::RemoveChunkMergerMode);
    auto removeTabletCellBundle = std::bind(doNotLockAndRemove, &TCompositeCypressNode::RemoveTabletCellBundle);
    auto removeChaosCellBundle = std::bind(doNotLockAndRemove, &TCompositeCypressNode::RemoveChaosCellBundle);
    auto removeMedia = std::bind(doNotLockAndRemove, &TCompositeCypressNode::RemoveMedia);
    auto removeHunkMedia = std::bind(doNotLockAndRemove, &TCompositeCypressNode::RemoveHunkMedia);
    auto removePrimaryMedium = std::bind(doRemovePrimaryMedium, /*isHunk*/ false);
    auto removeHunkPrimaryMedium = std::bind(doRemovePrimaryMedium, /*isHunk*/ true);

    switch (key) {
    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            doLockAndRemove(&TCompositeCypressNode::Remove##FieldName); \
            return true;
        FOR_EACH_SIMPLE_INHERITABLE_ATTRIBUTE(XX);
    #undef XX

    #define XX(FieldName, AttributeKey) \
        case EInternedAttributeKey::AttributeKey: \
            remove##AttributeKey(); \
            return true;
        FOR_EACH_COMPLICATED_INHERITABLE_ATTRIBUTE(XX)
    #undef XX

        default:
            break;
    }

    return TNontemplateCypressNodeProxyBase::RemoveBuiltinAttribute(key);
}

bool TNontemplateCompositeCypressNodeProxyBase::CanHaveChildren() const
{
    return true;
}

void TNontemplateCompositeCypressNodeProxyBase::AttachChild(TCypressNode* child)
{
    AttachChildToNode(TrunkNode_->As<TCompositeCypressNode>(), child);
    if (GetThisImpl()->GetReachable()) {
        SetReachableSubtreeNodes(child);
    }
}

void TNontemplateCompositeCypressNodeProxyBase::DetachChild(TCypressNode* child)
{
    if (GetThisImpl()->GetReachable()) {
        // It is important child has a parent when becoming unreachable for proper Sequoia syncs.
        // It is pretty fragile but I like it more than other options, it also makes sense to
        // set unreachable and remove parent in order opposite to AttachChild.
        SetUnreachableSubtreeNodes(child);
    }
    DetachChildFromNode(TrunkNode_->As<TCompositeCypressNode>(), child);
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
    TKeyToCypressNode keyToChildMapStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildMapStorage);
    auto keyToChildList = SortHashMapByKeys(keyToChildMap);

    // Take shared locks for children.
    using TChild = std::pair<std::string, TCypressNode*>;
    std::vector<TChild> children;
    children.reserve(keyToChildList.size());
    for (const auto& [key, child] : keyToChildList) {
        LockThisImpl(TLockRequest::MakeSharedChild(key));
        auto* childImpl = LockImpl(child);
        children.emplace_back(key, childImpl);
    }

    // Insert tombstones (if in transaction).
    for (const auto& [key, child] : children) {
        DoRemoveChild(impl, key, child);
    }

    SetModified(EModificationType::Content);
}

int TCypressMapNodeProxy::GetChildCount() const
{
    return NCypressServer::GetNodeChildCount(
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_);
}

std::vector<std::pair<std::string, INodePtr>> TCypressMapNodeProxy::GetChildren() const
{
    TKeyToCypressNode keyToChildStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildStorage);

    std::vector<std::pair<std::string, INodePtr>> result;
    result.reserve(keyToChildMap.size());
    for (const auto& [key, child] : keyToChildMap) {
        result.emplace_back(key, GetProxy(child));
    }

    return result;
}

std::vector<std::string> TCypressMapNodeProxy::GetKeys() const
{
    TKeyToCypressNode keyToChildStorage;
    const auto& keyToChildMap = GetMapNodeChildMap(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        &keyToChildStorage);
    return NYT::GetKeys(keyToChildMap);
}

INodePtr TCypressMapNodeProxy::FindChild(const std::string& key) const
{
    auto* trunkChildNode = FindMapNodeChild(
        Bootstrap_->GetCypressManager(),
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_,
        key);
    return trunkChildNode ? GetProxy(trunkChildNode) : nullptr;
}

bool TCypressMapNodeProxy::AddChild(const std::string& key, const NYTree::INodePtr& child)
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

    AttachChild(childImpl);

    SetModified(EModificationType::Content);

    return true;
}

bool TCypressMapNodeProxy::RemoveChild(const std::string& key)
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

    DetachChild(oldChildImpl);
    children.Set(key, newTrunkChildImpl);
    AttachChild(newChildImpl);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    securityManager->UpdateMasterMemoryUsage(impl);

    SetModified(EModificationType::Content);
}

std::optional<std::string> TCypressMapNodeProxy::FindChildKey(const IConstNodePtr& child) const
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
    const std::string& key,
    TCypressNode* childImpl)
{
    auto* trunkChildImpl = childImpl->GetTrunkNode();
    auto& children = impl->MutableChildren();
    if (Transaction_) {
        children.Set(key, nullptr);
    } else {
        children.Remove(key, trunkChildImpl);
    }
    DetachChild(childImpl);
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
        MakeShrunkFormattableView(
            attributeFilter,
            GetDynamicCypressManagerConfig()->MaxAttributeFilterSizeToLog),
        limit);

    TLimitedAsyncYsonWriter writer(context->GetReadRequestComplexityLimiter());

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    const auto& securityManager = Bootstrap_->GetSecurityManager();

    TKeyToCypressNode keyToChildMapStorage;
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
                response->set_value(ToProto(resultOrError.Value()));
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
    TInternedAttributeKey key,
    IYsonConsumer* consumer)
{
    auto* mapNode = GetThisImpl();
    const auto& Logger = CypressServerLogger;

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
        MakeShrunkFormattableView(
            attributeFilter,
            GetDynamicCypressManagerConfig()->MaxAttributeFilterSizeToLog));

    TLimitedAsyncYsonWriter writer(context->GetReadRequestComplexityLimiter());
    WriteAttributes(&writer, attributeFilter, false);
    writer.OnBeginMap();
    writer.OnEndMap();

    writer.Finish().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
        if (resultOrError.IsOK()) {
            response->set_value(ToProto(resultOrError.Value()));
            context->Reply();
        } else {
            context->Reply(resultOrError);
        }
    }));
}

void TSequoiaMapNodeProxy::ListSelf(
    TReqList* /*request*/,
    TRspList* /*response*/,
    const TCtxListPtr& context)
{
    // TODO(danilalexeev): Support list with attributes.
    context->SetRequestInfo();
    context->Reply();
}

int TSequoiaMapNodeProxy::GetChildCount() const
{
    return NCypressServer::GetNodeChildCount(
        TrunkNode_->As<TCypressMapNode>(),
        Transaction_);
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

} // namespace NYT::NCypressServer
