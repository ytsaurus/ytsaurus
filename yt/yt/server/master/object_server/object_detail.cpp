#include "object_detail.h"
#include "config.h"
#include "private.h"
#include "attribute_set.h"
#include "object_manager.h"
#include "object_service.h"
#include "type_handler_detail.h"
#include "path_resolver.h"
#include "yson_intern_registry.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/epoch_history_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/config.h>
#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/type_handler.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/config.h>
#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>
#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/client/hydra/version.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/async_consumer.h>
#include <yt/yt/core/yson/async_writer.h>
#include <yt/yt/core/yson/attribute_consumer.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/string/ascii.h>

namespace NYT::NObjectServer {

using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTransactionServer;

using NSecurityClient::ESecurityAction;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TObject* object)
    : Bootstrap_(bootstrap)
    , Metadata_(metadata)
    , Object_(object)
{
    YT_ASSERT(Bootstrap_);
    YT_ASSERT(Metadata_);
    YT_ASSERT(Object_);
}

TObjectId TObjectProxyBase::GetId() const
{
    return Object_->GetId();
}

TObject* TObjectProxyBase::GetObject() const
{
    return Object_;
}

TTransaction* TObjectProxyBase::GetTransaction() const
{
    return nullptr;
}

const IAttributeDictionary& TObjectProxyBase::Attributes() const
{
    return *const_cast<TObjectProxyBase*>(this)->GetCombinedAttributes();
}

IAttributeDictionary* TObjectProxyBase::MutableAttributes()
{
    return GetCombinedAttributes();
}

void TObjectProxyBase::SetModified(EModificationType modificationType)
{
    if (ModificationTrackingSuppressed_) {
        return;
    }

    if (!IsObjectAlive(Object_)) {
        return;
    }

    Object_->SetModified(modificationType);
}

void TObjectProxyBase::SuppressModificationTracking()
{
    ModificationTrackingSuppressed_ = true;
}

DEFINE_YPATH_SERVICE_METHOD(TObjectProxyBase, GetBasicAttributes)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    TGetBasicAttributesContext getBasicAttributesContext;
    if (request->has_permission()) {
        getBasicAttributesContext.Permission = CheckedEnumCast<EPermission>(request->permission());
    }
    if (request->has_columns()) {
        getBasicAttributesContext.Columns = FromProto<std::vector<TString>>(request->columns().items());
    }
    getBasicAttributesContext.OmitInaccessibleColumns = request->omit_inaccessible_columns();
    getBasicAttributesContext.PopulateSecurityTags = request->populate_security_tags();
    getBasicAttributesContext.ExternalCellTag = CellTagFromId(GetId());
    getBasicAttributesContext.ExternalTransactionId = GetObjectId(GetTransaction());

    GetBasicAttributes(&getBasicAttributesContext);

    ToProto(response->mutable_object_id(), GetId());
    response->set_external_cell_tag(ToProto<int>(getBasicAttributesContext.ExternalCellTag));
    if (getBasicAttributesContext.OmittedInaccessibleColumns) {
        ToProto(response->mutable_omitted_inaccessible_columns()->mutable_items(), *getBasicAttributesContext.OmittedInaccessibleColumns);
    }
    if (getBasicAttributesContext.SecurityTags) {
        ToProto(response->mutable_security_tags()->mutable_items(), getBasicAttributesContext.SecurityTags->Items);
    }

    ToProto(response->mutable_external_transaction_id(), getBasicAttributesContext.ExternalTransactionId);

    response->set_revision(getBasicAttributesContext.Revision);
    response->set_attribute_revision(getBasicAttributesContext.AttributeRevision);
    response->set_content_revision(getBasicAttributesContext.ContentRevision);

    context->SetResponseInfo("ExternalCellTag: %v, ExternalTransactionId: %v",
        getBasicAttributesContext.ExternalCellTag,
        getBasicAttributesContext.ExternalTransactionId);
    context->Reply();
}

void TObjectProxyBase::GetBasicAttributes(TGetBasicAttributesContext* context)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    if (context->Permission) {
        securityManager->ValidatePermission(Object_, *context->Permission);
    }

    context->Revision = Object_->GetRevision();
    context->AttributeRevision = Object_->GetAttributeRevision();
    context->ContentRevision = Object_->GetContentRevision();
}

DEFINE_YPATH_SERVICE_METHOD(TObjectProxyBase, CheckPermission)
{
    DeclareNonMutating();

    HandleCheckPermissionRequest(
        Bootstrap_,
        context,
        [&] (TUser* user, EPermission permission, TPermissionCheckOptions options) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            return securityManager->CheckPermission(Object_, user, permission, std::move(options));
        });
}

void TObjectProxyBase::Invoke(const IYPathServiceContextPtr& context)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();

    NProfiling::TWallTimer timer;

    TSupportsAttributes::Invoke(context);

    auto* counter = objectManager->GetMethodCumulativeExecuteTimeCounter(Object_->GetType(), context->GetMethod());
    counter->Add(timer.GetElapsedTime());
}

void TObjectProxyBase::DoWriteAttributesFragment(
    IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    bool stable)
{
    const auto& customAttributes = Attributes();

    if (attributeFilter) {
        auto keyToFilter = attributeFilter.Normalize();

        for (const auto& [key, pathFilter] : keyToFilter) {
            TAttributeValueConsumer attributeValueConsumer(consumer, key);

            TInternedAttributeKey internedKey;

            // Sync path.
            {
                auto filteringConsumer = TAttributeFilter::CreateFilteringConsumer(&attributeValueConsumer, pathFilter);

                if (auto value = customAttributes.FindYson(key)) {
                    filteringConsumer->GetConsumer()->OnRaw(value);
                    filteringConsumer->Finish();
                    continue;
                }

                internedKey = TInternedAttributeKey::Lookup(key);
                if (GetBuiltinAttribute(internedKey, filteringConsumer->GetConsumer())) {
                    filteringConsumer->Finish();
                    continue;
                }
            }

            // Async path.
            {
                auto asyncFilteringConsumer = TAttributeFilter::CreateAsyncFilteringConsumer(&attributeValueConsumer, pathFilter);
                if (auto asyncValue = GetBuiltinAttributeAsync(internedKey)) {
                    asyncFilteringConsumer->GetAsyncConsumer()->OnRaw(std::move(asyncValue));
                    asyncFilteringConsumer->Finish();
                    continue; // just for the symmetry
                }
            }
        }
    } else {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinAttributes;
        ListBuiltinAttributes(&builtinAttributes);

        auto userPairs = customAttributes.ListPairs();

        if (stable) {
            std::sort(
                userPairs.begin(),
                userPairs.end(),
                [] (const auto& lhs, const auto& rhs) {
                    return lhs.first < rhs.first;
                });
            std::sort(
                builtinAttributes.begin(),
                builtinAttributes.end(),
                [] (const ISystemAttributeProvider::TAttributeDescriptor& lhs, const ISystemAttributeProvider::TAttributeDescriptor& rhs) {
                    return lhs.InternedKey < rhs.InternedKey;
                });
        }

        for (const auto& [key, value] : userPairs) {
            consumer->OnKeyedItem(key);
            consumer->OnRaw(value);
        }

        for (const auto& descriptor : builtinAttributes) {
            auto key = descriptor.InternedKey;
            auto uninternedKey = key.Unintern();
            TAttributeValueConsumer attributeValueConsumer(consumer, uninternedKey);

            if (descriptor.Opaque) {
                attributeValueConsumer.OnEntity();
                continue;
            }

            if (GetBuiltinAttribute(key, &attributeValueConsumer)) {
                continue;
            }

            auto asyncValue = GetBuiltinAttributeAsync(key);
            if (asyncValue) {
                attributeValueConsumer.OnRaw(std::move(asyncValue));
                continue; // just for the symmetry
            }
        }
    }
}

bool TObjectProxyBase::ShouldHideAttributes()
{
    return true;
}

void TObjectProxyBase::BeforeInvoke(const IYPathServiceContextPtr& context)
{
    TYPathServiceBase::BeforeInvoke(context);

    const auto& requestHeader = context->RequestHeader();

    // Validate that mutating requests are only being invoked inside mutations or recovery.
    const auto& ypathExt = requestHeader.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    YT_VERIFY(!ypathExt.mutating() || NHydra::HasHydraContext());

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (requestHeader.HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
        const auto& prerequisitesExt = requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        objectManager->ValidatePrerequisites(prerequisitesExt);
    }

    for (const auto& additionalPath : ypathExt.additional_paths()) {
        TPathResolver resolver(
            Bootstrap_,
            context->GetService(),
            context->GetMethod(),
            additionalPath,
            GetTransactionId(context));
        auto result = resolver.Resolve();
        if (std::holds_alternative<TPathResolver::TRemoteObjectPayload>(result.Payload)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::CrossCellAdditionalPath,
                "Request is cross-cell since it involves target path %v and additional path %v",
                ypathExt.original_target_path(),
                additionalPath);
        }
    }

    const auto& prerequisitesExt = requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
    for (const auto& prerequisite : prerequisitesExt.revisions()) {
        const auto& prerequisitePath = prerequisite.path();
        TPathResolver resolver(
            Bootstrap_,
            context->GetService(),
            context->GetMethod(),
            prerequisitePath,
            GetTransactionId(context));
        auto result = resolver.Resolve();
        if (std::holds_alternative<TPathResolver::TRemoteObjectPayload>(result.Payload)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::CrossCellRevisionPrerequisitePath,
                "Request is cross-cell since it involves target path %v and revision prerequisite path %v",
                ypathExt.original_target_path(),
                prerequisitePath);
        }
    }

    {
        TStringBuilder builder;
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        delimitedBuilder->AppendFormat("TargetObjectId: %v", GetVersionedId());

        if (!ypathExt.target_path().empty()) {
            delimitedBuilder->AppendFormat("RequestPathSuffix: %v", ypathExt.target_path());
        }

        context->SetRawRequestInfo(builder.Flush(), true);
    }

    ModificationTrackingSuppressed_ = GetSuppressModificationTracking(requestHeader);
}

bool TObjectProxyBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetBasicAttributes);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    DISPATCH_YPATH_SERVICE_METHOD(CheckPermission);
    DISPATCH_YPATH_SERVICE_METHOD(MultisetAttributes);
    return TYPathServiceBase::DoInvoke(context);
}

void TObjectProxyBase::SetAttribute(
    const NYTree::TYPath& path,
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    TSupportsAttributes::SetAttribute(path, request, response, context);
    ReplicateAttributeUpdate(context);
}

void TObjectProxyBase::RemoveAttribute(
    const NYTree::TYPath& path,
    TReqRemove* request,
    TRspRemove* response,
    const TCtxRemovePtr& context)
{
    TSupportsAttributes::RemoveAttribute(path, request, response, context);
    ReplicateAttributeUpdate(context);
}

void TObjectProxyBase::SetAttributes(
    const TYPath& path,
    TReqMultisetAttributes* request,
    TRspMultisetAttributes* response,
    const TCtxMultisetAttributesPtr& context)
{
    TSupportsAttributes::SetAttributes(path, request, response, context);
    ReplicateAttributeUpdate(context);
}

void TObjectProxyBase::ReplicateAttributeUpdate(const IServiceContextPtr& context)
{
    // XXX(babenko): make more objects foreign and replace with IsForeign
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (Object_->GetNativeCellTag() != multicellManager->GetCellTag()) {
        return;
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& handler = objectManager->GetHandler(Object_->GetType());
    auto flags = handler->GetFlags();

    if (None(flags & ETypeFlags::ReplicateAttributes)) {
        return;
    }

    ExternalizeToMasters(std::move(context), handler->GetReplicationCellTags(Object_));
}

IAttributeDictionary* TObjectProxyBase::GetCustomAttributes()
{
    YT_ASSERT(CustomAttributes_);
    return CustomAttributes_;
}

ISystemAttributeProvider* TObjectProxyBase::GetBuiltinAttributeProvider()
{
    return this;
}

void TObjectProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    auto* acd = FindThisAcd();
    bool hasAcd = acd;
    bool hasOwner = acd && acd->GetOwner();
    bool isForeign = Object_->IsForeign();
    bool isSequoia = Object_->IsSequoia();

    descriptors->push_back(EInternedAttributeKey::Id);
    descriptors->push_back(EInternedAttributeKey::Type);
    descriptors->push_back(EInternedAttributeKey::Builtin);
    descriptors->push_back(EInternedAttributeKey::Sequoia);
    descriptors->push_back(EInternedAttributeKey::RefCounter);
    descriptors->push_back(EInternedAttributeKey::EphemeralRefCounter);
    descriptors->push_back(EInternedAttributeKey::WeakRefCounter);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ImportRefCounter)
        .SetPresent(isForeign));
    descriptors->push_back(EInternedAttributeKey::Foreign);
    descriptors->push_back(EInternedAttributeKey::NativeCellTag);
    descriptors->push_back(EInternedAttributeKey::Revision);
    descriptors->push_back(EInternedAttributeKey::AttributeRevision);
    descriptors->push_back(EInternedAttributeKey::ContentRevision);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::InheritAcl)
        .SetPresent(hasAcd)
        .SetWritable(true)
        .SetWritePermission(EPermission::Administer)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Acl)
        .SetPresent(hasAcd)
        .SetWritable(true)
        .SetWritePermission(EPermission::Administer)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Owner)
        .SetWritable(true)
        .SetPresent(hasOwner));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EffectiveAcl)
        .SetOpaque(true));
    descriptors->push_back(EInternedAttributeKey::UserAttributeKeys);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OpaqueAttributeKeys)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UserAttributes)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LifeStage)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EstimatedCreationTime)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Aevum)
        .SetPresent(isSequoia));
}

const THashSet<TInternedAttributeKey>& TObjectProxyBase::GetBuiltinAttributeKeys()
{
    return Metadata_->SystemBuiltinAttributeKeysCache.GetBuiltinAttributeKeys(this);
}

bool TObjectProxyBase::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();

    bool isForeign = Object_->IsForeign();
    auto* acd = FindThisAcd();

    switch (key) {
        case EInternedAttributeKey::Id:
            BuildYsonFluently(consumer)
                .Value(ToString(GetId()));
            return true;

        case EInternedAttributeKey::Type:
            BuildYsonFluently(consumer)
                .Value(TypeFromId(GetId()));
            return true;

        case EInternedAttributeKey::Builtin:
            BuildYsonFluently(consumer)
                .Value(Object_->IsBuiltin());
            return true;

        case EInternedAttributeKey::Sequoia:
            BuildYsonFluently(consumer)
                .Value(Object_->IsSequoia());
            return true;

        case EInternedAttributeKey::RefCounter:
            BuildYsonFluently(consumer)
                .Value(Object_->GetObjectRefCounter());
            return true;

        case EInternedAttributeKey::EphemeralRefCounter:
            BuildYsonFluently(consumer)
                .Value(Object_->GetObjectEphemeralRefCounter());
            return true;

        case EInternedAttributeKey::WeakRefCounter:
            BuildYsonFluently(consumer)
                .Value(Object_->GetObjectWeakRefCounter());
            return true;

        case EInternedAttributeKey::ImportRefCounter:
            if (!isForeign) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(Object_->GetImportRefCounter());
            return true;

        case EInternedAttributeKey::Foreign:
            BuildYsonFluently(consumer)
                .Value(isForeign);
            return true;

        case EInternedAttributeKey::NativeCellTag:
            BuildYsonFluently(consumer)
                .Value(Object_->GetNativeCellTag());
            return true;

        case EInternedAttributeKey::Revision:
            BuildYsonFluently(consumer)
                .Value(Object_->GetRevision());
            return true;

        case EInternedAttributeKey::AttributeRevision:
            BuildYsonFluently(consumer)
                .Value(Object_->GetAttributeRevision());
            return true;

        case EInternedAttributeKey::ContentRevision:
            BuildYsonFluently(consumer)
                .Value(Object_->GetContentRevision());
            return true;

        case EInternedAttributeKey::InheritAcl:
            if (!acd) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(acd->Inherit());
            return true;

        case EInternedAttributeKey::Acl:
            if (!acd) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(acd->Acl());
            return true;

        case EInternedAttributeKey::Owner:
            if (!acd || !acd->GetOwner()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(acd->GetOwner()->GetName());
            return true;

        case EInternedAttributeKey::EffectiveAcl:
            BuildYsonFluently(consumer)
                .Value(securityManager->GetEffectiveAcl(Object_));
            return true;

        case EInternedAttributeKey::UserAttributeKeys: {
            auto customKeys = GetCustomAttributes()->ListKeys();
            const auto& systemCustomKeys = Metadata_->SystemCustomAttributeKeysCache.GetCustomAttributeKeys(this);
            consumer->OnBeginList();
            for (const auto& key : customKeys) {
                if (!systemCustomKeys.contains(key)) {
                    consumer->OnListItem();
                    consumer->OnStringScalar(key);
                }
            }
            consumer->OnEndList();
            return true;
        }

        case EInternedAttributeKey::OpaqueAttributeKeys: {
            const auto& opaqueKeys = Metadata_->OpaqueAttributeKeysCache.GetOpaqueAttributeKeys(this);
            BuildYsonFluently(consumer)
                .Value(opaqueKeys);
            return true;
        }

        case EInternedAttributeKey::UserAttributes: {
            auto customPairs = GetCustomAttributes()->ListPairs();
            const auto& systemCustomKeys = Metadata_->SystemCustomAttributeKeysCache.GetCustomAttributeKeys(this);
            consumer->OnBeginMap();
            for (const auto& [key, value] : customPairs) {
                if (!systemCustomKeys.contains(key)) {
                    consumer->OnKeyedItem(key);
                    consumer->OnRaw(value);
                }
            }
            consumer->OnEndMap();
            return true;
        }

        case EInternedAttributeKey::LifeStage:
            BuildYsonFluently(consumer)
                .Value(Object_->GetLifeStage());
            return true;

        case EInternedAttributeKey::EstimatedCreationTime: {
            const auto& epochHistoryManager = Bootstrap_->GetEpochHistoryManager();
            auto [minTime, maxTime] = epochHistoryManager->GetEstimatedCreationTime(
                GetId(),
                NHydra::HasMutationContext()
                    ? NHydra::GetCurrentMutationContext()->GetTimestamp()
                    : NProfiling::GetInstant());
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("min").Value(minTime)
                    .Item("max").Value(maxTime)
                .EndMap();
            return true;
        }

        case EInternedAttributeKey::Aevum: {
            if (!Object_->IsSequoia()) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(Object_->GetAevum());
            return true;
        }

        default:
            break;
    }

    return false;
}

TFuture<TYsonString> TObjectProxyBase::GetBuiltinAttributeAsync(TInternedAttributeKey /*key*/)
{
    return std::nullopt;
}

bool TObjectProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    auto securityManager = Bootstrap_->GetSecurityManager();
    auto* acd = FindThisAcd();
    if (!acd) {
        return false;
    }

    switch (key) {
        case EInternedAttributeKey::InheritAcl: {
            ValidateNoTransaction();

            auto inherit = ConvertTo<bool>(value);

            if (!force) {
                ValidateModifiedPermission(TAcdOverride(inherit));
            }

            if (inherit != acd->Inherit()) {
                acd->SetInherit(inherit);

                LogAcdUpdate(key, value);

                SetModified(EModificationType::Attributes);
            }

            return true;
        }

        case EInternedAttributeKey::Acl: {
            ValidateNoTransaction();

            auto newAcl = DeserializeAclOrThrow(ConvertToNode(value), securityManager);
            if (!force) {
                ValidateModifiedPermission(TAcdOverride(newAcl));
            }

            acd->SetEntries(newAcl);

            LogAcdUpdate(key, value);

            SetModified(EModificationType::Attributes);

            return true;
        }

        case EInternedAttributeKey::Owner: {
            ValidateNoTransaction();

            auto name = ConvertTo<TString>(value);
            auto* owner = securityManager->GetSubjectByNameOrAliasOrThrow(name, true /*activeLifeStageOnly*/);
            auto* user = securityManager->GetAuthenticatedUser();
            if (user != owner && !securityManager->IsSuperuser(user)) {
                THROW_ERROR_EXCEPTION(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: can only set owner to self");
            }

            if (!force) {
                ValidateModifiedPermission(TAcdOverride(*owner));
            }

            acd->SetOwner(owner);

            LogAcdUpdate(key, value);

            SetModified(EModificationType::Attributes);

            return true;
        }

        default:
            break;
    }

    return false;
}

bool TObjectProxyBase::RemoveBuiltinAttribute(TInternedAttributeKey /*key*/)
{
    return false;
}

void TObjectProxyBase::LogAcdUpdate(TInternedAttributeKey /*key*/, const TYsonString& /*value*/)
{ }

void TObjectProxyBase::ValidateCustomAttributeUpdate(
    const TString& key,
    const TYsonString& /*oldValue*/,
    const TYsonString& newValue)
{
    if (!newValue) {
        return;
    }

    const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->ObjectManager;
    const auto& reservedAttributes = config->ReservedAttributes;

    auto it = reservedAttributes.find(Object_->GetType());
    if (it == reservedAttributes.end()) {
        return;
    }

    auto jt = it->second.find(key);
    if (jt != it->second.end()) {
        THROW_ERROR_EXCEPTION("Attribute %Qv is deprecated or reserved for future use: %v",
            key,
            jt->second);
    }
}

void TObjectProxyBase::GuardedValidateCustomAttributeUpdate(
    const TString& key,
    const TYsonString& oldValue,
    const TYsonString& newValue)
{
    try {
        if (newValue) {
            ValidateCustomAttributeLength(newValue);
        }
        ValidateCustomAttributeUpdate(key, oldValue, newValue);
    } catch (const std::exception& ex) {
        if (newValue) {
            THROW_ERROR_EXCEPTION("Error setting custom attribute %Qv",
                ToYPathLiteral(key))
                << ex;
        } else {
            THROW_ERROR_EXCEPTION("Error removing custom attribute %Qv",
                ToYPathLiteral(key))
                << ex;
        }
    }
}

void TObjectProxyBase::ValidateCustomAttributeLength(const TYsonString& value)
{
    auto size = std::ssize(value.AsStringBuf());
    auto limit = GetDynamicCypressManagerConfig()->MaxAttributeSize;
    if (size > limit) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::MaxAttributeSizeViolation,
            "Attribute size limit exceeded: %v > %v",
            size,
            limit);
    }
}

void TObjectProxyBase::DeclareMutating()
{
    YT_VERIFY(NHydra::HasHydraContext());
}

void TObjectProxyBase::DeclareNonMutating()
{ }

void TObjectProxyBase::ValidateTransaction()
{
    if (!GetVersionedId().IsBranched()) {
        THROW_ERROR_EXCEPTION("Operation cannot be performed outside of a transaction");
    }
}

void TObjectProxyBase::ValidateNoTransaction()
{
    if (GetVersionedId().IsBranched()) {
        THROW_ERROR_EXCEPTION("Operation cannot be performed in transaction");
    }
}

void TObjectProxyBase::ValidatePermission(EPermissionCheckScope scope, EPermission permission, const TString& /*user*/)
{
    YT_VERIFY(scope == EPermissionCheckScope::This);
    ValidatePermission(Object_, permission);
}

void TObjectProxyBase::ValidatePermission(TObject* object, EPermission permission)
{
    YT_VERIFY(object);
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();
    securityManager->ValidatePermission(object, user, permission);
}

std::unique_ptr<IPermissionValidator> TObjectProxyBase::CreatePermissionValidator()
{
    return std::make_unique<TPermissionValidator>(this);
}

void TObjectProxyBase::ValidateAnnotation(const TString& annotation)
{
    if (annotation.size() > MaxAnnotationLength) {
        THROW_ERROR_EXCEPTION("Annotation is too long")
            << TErrorAttribute("annotation_length", annotation.size())
            << TErrorAttribute("maximum_annotation_length", MaxAnnotationLength);
    }

    auto isAsciiText = [] (char c) {
        return IsAsciiAlnum(c) || IsAsciiSpace(c) || IsAsciiPunct(c);
    };

    if (!::AllOf(annotation.begin(), annotation.end(), isAsciiText)) {
        THROW_ERROR_EXCEPTION("Only ASCII alphanumeric, white-space and punctuation characters are allowed in annotations");
    }
}

bool TObjectProxyBase::IsRecovery() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

bool TObjectProxyBase::IsLeader() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsLeader();
}

bool TObjectProxyBase::IsFollower() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsFollower();
}

bool TObjectProxyBase::IsPrimaryMaster() const
{
    return Bootstrap_->GetMulticellManager()->IsPrimaryMaster();
}

bool TObjectProxyBase::IsSecondaryMaster() const
{
    return Bootstrap_->GetMulticellManager()->IsSecondaryMaster();
}

void TObjectProxyBase::RequireLeader() const
{
    Bootstrap_->GetHydraFacade()->RequireLeader();
}

void TObjectProxyBase::PostToSecondaryMasters(IServiceContextPtr context)
{
    auto* object = GetObject();
    YT_VERIFY(object->IsNative());

    auto* transaction = GetTransaction();

    ClearPrerequisiteTransactions(context);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToSecondaryMasters(
        TCrossCellMessage(object->GetId(), GetObjectId(transaction), std::move(context)));
}

void TObjectProxyBase::ExternalizeToMasters(IServiceContextPtr context, const TCellTagList& cellTags)
{
    auto* object = GetObject();
    YT_VERIFY(object->IsNative());

    auto* transaction = GetTransaction();

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    auto externalizedTransactionId = transactionManager->ExternalizeTransaction(transaction, cellTags);

    ClearPrerequisiteTransactions(context);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMasters(
        TCrossCellMessage(object->GetId(), externalizedTransactionId, std::move(context)),
        cellTags);
}

void TObjectProxyBase::ClearPrerequisiteTransactions(NRpc::IServiceContextPtr& context)
{
    if (context->RequestHeader().HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
        auto* prerequisitesExt = context->RequestHeader().MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        prerequisitesExt->Clear();
    }
}

void TObjectProxyBase::ValidateModifiedPermission(TAcdOverride acdOverride)
{
    auto forbidUnforcedIrreversibleAclChanges = Bootstrap_
        ->GetConfigManager()
        ->GetConfig()
        ->SecurityManager
        ->ForbidIrreversibleAclChanges;

    if (!forbidUnforcedIrreversibleAclChanges) {
        return;
    }
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto result = securityManager->CheckPermission(
        Object_,
        securityManager->GetAuthenticatedUser(),
        EPermission::Administer,
        {.FirstObjectAcdOverride = std::move(acdOverride)});
    YT_VERIFY(result.Action == ESecurityAction::Allow || result.Action == ESecurityAction::Deny);
    if (result.Action == ESecurityAction::Deny) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::IrreversibleAclModification,
            "It will be impossible for this user to revert this modification, the \"force\" option should be specified");
    }
}

const NCypressServer::TDynamicCypressManagerConfigPtr& TObjectProxyBase::GetDynamicCypressManagerConfig() const
{
    return Bootstrap_->GetConfigManager()->GetConfig()->CypressManager;
}

////////////////////////////////////////////////////////////////////////////////

TNontemplateNonversionedObjectProxyBase::TCustomAttributeDictionary::TCustomAttributeDictionary(
    TNontemplateNonversionedObjectProxyBase* proxy)
    : Proxy_(proxy)
{ }

std::vector<TString> TNontemplateNonversionedObjectProxyBase::TCustomAttributeDictionary::ListKeys() const
{
    const auto* object = Proxy_->Object_;
    const auto* attributes = object->GetAttributes();
    std::vector<TString> keys;
    if (attributes) {
        keys.reserve(attributes->Attributes().size());
        for (const auto& [key, value] : attributes->Attributes()) {
            // Attribute cannot be empty (i.e. deleted) in null transaction.
            YT_ASSERT(value);
            keys.push_back(key);
        }
    }
    return keys;
}

std::vector<IAttributeDictionary::TKeyValuePair> TNontemplateNonversionedObjectProxyBase::TCustomAttributeDictionary::ListPairs() const
{
    const auto* object = Proxy_->Object_;
    const auto* attributes = object->GetAttributes();
    std::vector<TKeyValuePair> pairs;
    if (attributes) {
        pairs.reserve(attributes->Attributes().size());
        for (const auto& [key, value] : attributes->Attributes()) {
            // Attribute cannot be empty (i.e. deleted) in null transaction.
            YT_ASSERT(value);
            pairs.emplace_back(key, value);
        }
    }
    return pairs;
}

TYsonString TNontemplateNonversionedObjectProxyBase::TCustomAttributeDictionary::FindYson(TStringBuf key) const
{
    const auto* object = Proxy_->Object_;
    const auto* attributes = object->GetAttributes();
    if (!attributes) {
        return {};
    }

    auto it = attributes->Attributes().find(key);
    if (it == attributes->Attributes().end()) {
        return {};
    }

    // Attribute cannot be empty (i.e. deleted) in null transaction.
    YT_ASSERT(it->second);
    return it->second;
}

void TNontemplateNonversionedObjectProxyBase::TCustomAttributeDictionary::SetYson(const TString& key, const TYsonString& value)
{
    auto oldValue = FindYson(key);
    Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, value);

    auto* object = Proxy_->Object_;
    auto* attributes = object->GetMutableAttributes();
    const auto& ysonInternRegistry = Proxy_->Bootstrap_->GetYsonInternRegistry();
    attributes->Set(key, ysonInternRegistry->Intern(value));

    Proxy_->SetModified(EModificationType::Attributes);
}

bool TNontemplateNonversionedObjectProxyBase::TCustomAttributeDictionary::Remove(const TString& key)
{
    auto oldValue = FindYson(key);
    Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, TYsonString());

    auto* object = Proxy_->Object_;
    if (!object->GetAttributes()) {
        return false;
    }

    auto* attributes = object->GetMutableAttributes();

    auto it = attributes->Attributes().find(key);
    if (it == attributes->Attributes().end()) {
        return false;
    }

    // Attribute cannot be empty (i.e. deleted) in null transaction.
    YT_ASSERT(it->second);
    attributes->Remove(key);
    if (attributes->Attributes().empty()) {
        object->ClearAttributes();
    }

    Proxy_->SetModified(EModificationType::Attributes);

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TNontemplateNonversionedObjectProxyBase::TNontemplateNonversionedObjectProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TObject* object)
    : TObjectProxyBase(bootstrap, metadata, object)
    , CustomAttributesImpl_(New<TCustomAttributeDictionary>(this))
{
    CustomAttributes_ = CustomAttributesImpl_.Get();
}

bool TNontemplateNonversionedObjectProxyBase::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    return TObjectProxyBase::DoInvoke(context);
}

void TNontemplateNonversionedObjectProxyBase::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    context->SetRequestInfo();

    auto attributeFilter = request->has_attributes()
        ? FromProto<TAttributeFilter>(request->attributes())
        : TAttributeFilter();

    if (attributeFilter) {
        TAsyncYsonWriter writer;
        WriteAttributes(&writer, attributeFilter, false);
        writer.OnEntity();

        writer.Finish()
            .Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
                if (resultOrError.IsOK()) {
                    response->set_value(resultOrError.Value().ToString());
                    context->Reply();
                } else {
                    context->Reply(resultOrError);
                }
            }));
    } else {
        response->set_value("#");
        context->Reply();
    }
}

void TNontemplateNonversionedObjectProxyBase::ValidateRemoval()
{
    THROW_ERROR_EXCEPTION("Object cannot be removed explicitly");
}

void TNontemplateNonversionedObjectProxyBase::RemoveSelf(
    TReqRemove* /*request*/,
    TRspRemove* /*response*/,
    const TCtxRemovePtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    ValidateRemoval();

    context->SetRequestInfo();

    const auto& objectManager = Bootstrap_->GetObjectManager();
    objectManager->RemoveObject(Object_);

    context->Reply();
}

TVersionedObjectId TNontemplateNonversionedObjectProxyBase::GetVersionedId() const
{
    return TVersionedObjectId(Object_->GetId());
}

TAccessControlDescriptor* TNontemplateNonversionedObjectProxyBase::FindThisAcd()
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->FindAcd(Object_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

