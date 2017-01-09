#include "object_detail.h"
#include "private.h"
#include "attribute_set.h"
#include "object_manager.h"
#include "object_service.h"
#include "type_handler_detail.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/cypress_server/virtual.h>

#include <yt/server/election/election_manager.h>

#include <yt/server/hydra/mutation_context.h>

#include <yt/server/object_server/object_manager.h>
#include <yt/server/object_server/type_handler.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/acl.h>
#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>
#include <yt/server/security_server/group.h>
#include <yt/server/security_server/security_manager.pb.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/string.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/rpc.pb.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/exception_helpers.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/string.h>
#include <yt/core/yson/async_consumer.h>
#include <yt/core/yson/attribute_consumer.h>

namespace NYT {
namespace NObjectServer {

using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TObjectProxyBase::TCustomAttributeDictionary
    : public IAttributeDictionary
{
public:
    explicit TCustomAttributeDictionary(TObjectProxyBase* proxy)
        : Proxy_(proxy)
    { }

    // IAttributeDictionary members
    virtual std::vector<Stroka> List() const override
    {
        const auto* object = Proxy_->Object_;
        const auto* attributes = object->GetAttributes();
        std::vector<Stroka> keys;
        if (attributes) {
            for (const auto& pair : attributes->Attributes()) {
                // Attribute cannot be empty (i.e. deleted) in null transaction.
                Y_ASSERT(pair.second);
                keys.push_back(pair.first);
            }
        }
        return keys;
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const override
    {
        const auto* object = Proxy_->Object_;
        const auto* attributes = object->GetAttributes();
        if (!attributes) {
            return Null;
        }

        auto it = attributes->Attributes().find(key);
        if (it == attributes->Attributes().end()) {
            return Null;
        }

        // Attribute cannot be empty (i.e. deleted) in null transaction.
        Y_ASSERT(it->second);
        return it->second;
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        auto oldValue = FindYson(key);
        Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, value);

        auto* object = Proxy_->Object_;
        auto* attributes = object->GetMutableAttributes();
        attributes->Attributes()[key] = value;
    }

    virtual bool Remove(const Stroka& key) override
    {
        auto oldValue = FindYson(key);
        Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, Null);

        auto* object = Proxy_->Object_;
        auto* attributes = object->GetMutableAttributes();
        if (!attributes) {
            return false;
        }

        auto it = attributes->Attributes().find(key);
        if (it == attributes->Attributes().end()) {
            return false;
        }

        // Attribute cannot be empty (i.e. deleted) in null transaction.
        Y_ASSERT(it->second);
        attributes->Attributes().erase(it);
        if (attributes->Attributes().empty()) {
            object->ClearAttributes();
        }

        return true;
    }

private:
    TObjectProxyBase* const Proxy_;

};

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TObjectBase* object)
    : Bootstrap_(bootstrap)
    , Metadata_(metadata)
    , Object_(object)
{
    Y_ASSERT(Bootstrap_);
    Y_ASSERT(Metadata_);
    Y_ASSERT(Object_);
}

const TObjectId& TObjectProxyBase::GetId() const
{
    return Object_->GetId();
}

const IAttributeDictionary& TObjectProxyBase::Attributes() const
{
    return *const_cast<TObjectProxyBase*>(this)->GetCombinedAttributes();
}

IAttributeDictionary* TObjectProxyBase::MutableAttributes()
{
    return GetCombinedAttributes();
}

DEFINE_YPATH_SERVICE_METHOD(TObjectProxyBase, GetBasicAttributes)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    auto permissions = EPermissionSet(request->permissions());
    for (auto permission : TEnumTraits<EPermission>::GetDomainValues()) {
        if (Any(permissions & permission)) {
            ValidatePermission(EPermissionCheckScope::This, permission);
        }
    }

    ToProto(response->mutable_object_id(), GetId());

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& handler = objectManager->GetHandler(Object_);
    auto replicationCellTags = handler->GetReplicationCellTags(Object_);
    // TODO(babenko): this only works properly for chunk owners
    response->set_cell_tag(replicationCellTags.empty() ? Bootstrap_->GetCellTag() : replicationCellTags[0]);

    context->SetResponseInfo();
    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TObjectProxyBase, CheckPermission)
{
    DeclareNonMutating();

    auto userName = request->user();
    auto permission = EPermission(request->permission());
    context->SetRequestInfo("User: %v, Permission: %v",
        userName,
        permission);

    const auto& objectManager = Bootstrap_->GetObjectManager();

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetUserByNameOrThrow(userName);

    auto result = securityManager->CheckPermission(Object_, user, permission);

    response->set_action(static_cast<int>(result.Action));
    if (result.Object) {
        ToProto(response->mutable_object_id(), result.Object->GetId());
        const auto& handler = objectManager->GetHandler(result.Object);
        response->set_object_name(handler->GetName(result.Object));
    }
    if (result.Subject) {
        ToProto(response->mutable_subject_id(), result.Subject->GetId());
        response->set_subject_name(result.Subject->GetName());
    }

    context->SetResponseInfo("Action: %v", result.Action);
    context->Reply();
}

void TObjectProxyBase::Invoke(const IServiceContextPtr& context)
{
    const auto& requestHeader = context->RequestHeader();

    // Validate that mutating requests are only being invoked inside mutations or recovery.
    const auto& ypathExt = requestHeader.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    YCHECK(!ypathExt.mutating() || NHydra::HasMutationContext());

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (requestHeader.HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
        const auto& prerequiesitesExt = requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        objectManager->ValidatePrerequisites(prerequiesitesExt);
    }

    LOG_DEBUG_UNLESS(IsRecovery(), "Invoke: %v:%v %v (ObjectId: %v, RequestId: %v, User: %v)",
        context->GetService(),
        context->GetMethod(),
        ypathExt.path(),
        GetVersionedId(),
        context->GetRequestId(),
        user->GetName());

    NProfiling::TTagIdList tagIds{
        objectManager->GetTypeTagId(Object_->GetType()),
        objectManager->GetMethodTagId(context->GetMethod())
    };
    const auto& Profiler = objectManager->GetProfiler();
    static const auto profilingPath = TYPath("/verb_execute_time");
    PROFILE_TIMING (profilingPath, tagIds) {
        TSupportsAttributes::Invoke(context);
    }
}

void TObjectProxyBase::WriteAttributesFragment(
    IAsyncYsonConsumer* consumer,
    const TNullable<std::vector<Stroka>>& attributeKeys,
    bool sortKeys)
{
    const auto& customAttributes = Attributes();

    if (attributeKeys) {
        auto keys = *attributeKeys;

        if (sortKeys) {
            std::sort(keys.begin(), keys.end());
        }

        for (const auto& key : keys) {
            TAttributeValueConsumer attributeValueConsumer(consumer, key);

            auto value = customAttributes.FindYson(key);
            if (value) {
                attributeValueConsumer.OnRaw(*value);
                continue;
            }

            if (GetBuiltinAttribute(key, &attributeValueConsumer))
                continue;

            auto asyncValue = GetBuiltinAttributeAsync(key);
            if (asyncValue) {
                attributeValueConsumer.OnRaw(std::move(asyncValue));
                continue; // just for the symmetry
            }
        }
    } else {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinAttributes;
        ListBuiltinAttributes(&builtinAttributes);

        auto userKeys = customAttributes.List();

        // TODO(babenko): this is not exactly totally sorted keys, but should be fine.
        if (sortKeys) {
            std::sort(
                userKeys.begin(),
                userKeys.end());

            std::sort(
                builtinAttributes.begin(),
                builtinAttributes.end(),
                [] (const ISystemAttributeProvider::TAttributeDescriptor& lhs, const ISystemAttributeProvider::TAttributeDescriptor& rhs) {
                    return lhs.Key < rhs.Key;
                });
        }

        for (const auto& key : userKeys) {
            auto value = customAttributes.GetYson(key);
            consumer->OnKeyedItem(key);
            consumer->OnRaw(value);
        }

        for (const auto& descriptor : builtinAttributes) {
            auto key = Stroka(descriptor.Key);
            TAttributeValueConsumer attributeValueConsumer(consumer, key);

            if (descriptor.Opaque) {
                attributeValueConsumer.OnEntity();
                continue;
            }

            if (GetBuiltinAttribute(descriptor.Key, &attributeValueConsumer))
                continue;

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

bool TObjectProxyBase::DoInvoke(const IServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetBasicAttributes);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    DISPATCH_YPATH_SERVICE_METHOD(Exists);
    DISPATCH_YPATH_SERVICE_METHOD(CheckPermission);
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

void TObjectProxyBase::ReplicateAttributeUpdate(IServiceContextPtr context)
{
    if (!IsPrimaryMaster())
        return;

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& handler = objectManager->GetHandler(Object_->GetType());
    auto flags = handler->GetFlags();

    if (None(flags & ETypeFlags::ReplicateAttributes))
        return;

    auto replicationCellTags = handler->GetReplicationCellTags(Object_);
    PostToMasters(std::move(context), replicationCellTags);
}

IAttributeDictionary* TObjectProxyBase::GetCustomAttributes()
{
    if (!CustomAttributes_) {
        CustomAttributes_ = DoCreateCustomAttributes();
    }
    return CustomAttributes_.get();
}

ISystemAttributeProvider* TObjectProxyBase::GetBuiltinAttributeProvider()
{
    return this;
}

std::unique_ptr<IAttributeDictionary> TObjectProxyBase::DoCreateCustomAttributes()
{
    return std::unique_ptr<IAttributeDictionary>(new TCustomAttributeDictionary(this));
}

void TObjectProxyBase::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    auto* acd = FindThisAcd();
    bool hasAcd = acd;
    bool hasOwner = acd && acd->GetOwner();
    bool isForeign = Object_->IsForeign();

    descriptors->push_back("id");
    descriptors->push_back("type");
    descriptors->push_back("builtin");
    descriptors->push_back("ref_counter");
    descriptors->push_back("weak_ref_counter");
    descriptors->push_back(TAttributeDescriptor("import_ref_counter")
        .SetPresent(isForeign));
    descriptors->push_back("foreign");
    descriptors->push_back(TAttributeDescriptor("inherit_acl")
        .SetPresent(hasAcd)
        .SetWritePermission(EPermission::Administer)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("acl")
        .SetPresent(hasAcd)
        .SetWritePermission(EPermission::Administer)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("owner")
        .SetPresent(hasOwner));
    descriptors->push_back(TAttributeDescriptor("effective_acl")
        .SetOpaque(true));
}

const yhash_set<const char*>& TObjectProxyBase::GetBuiltinAttributeKeys()
{
    return Metadata_->BuiltinAttributeKeysCache.GetBuiltinAttributeKeys(this);
}

bool TObjectProxyBase::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto& objectManager = Bootstrap_->GetObjectManager();

    bool isForeign = Object_->IsForeign();

    if (key == "id") {
        BuildYsonFluently(consumer)
            .Value(ToString(GetId()));
        return true;
    }

    if (key == "type") {
        BuildYsonFluently(consumer)
            .Value(TypeFromId(GetId()));
        return true;
    }

    if (key == "builtin") {
        BuildYsonFluently(consumer)
            .Value(Object_->IsBuiltin());
        return true;
    }

    if (key == "ref_counter") {
        BuildYsonFluently(consumer)
            .Value(objectManager->GetObjectRefCounter(Object_));
        return true;
    }

    if (key == "weak_ref_counter") {
        BuildYsonFluently(consumer)
            .Value(objectManager->GetObjectWeakRefCounter(Object_));
        return true;
    }

    if (isForeign && key == "import_ref_counter") {
        BuildYsonFluently(consumer)
            .Value(Object_->GetImportRefCounter());
        return true;
    }

    if (key == "foreign") {
        BuildYsonFluently(consumer)
            .Value(isForeign);
        return true;
    }

    auto* acd = FindThisAcd();
    if (acd) {
        if (key == "inherit_acl") {
            BuildYsonFluently(consumer)
                .Value(acd->GetInherit());
            return true;
        }

        if (key == "acl") {
            BuildYsonFluently(consumer)
                .Value(acd->Acl());
            return true;
        }

        if (key == "owner" && acd->GetOwner()) {
            BuildYsonFluently(consumer)
                .Value(acd->GetOwner()->GetName());
            return true;
        }
    }

    if (key == "effective_acl") {
        BuildYsonFluently(consumer)
            .Value(securityManager->GetEffectiveAcl(Object_));
        return true;
    }

    return false;
}

TFuture<TYsonString> TObjectProxyBase::GetBuiltinAttributeAsync(const Stroka& /*key*/)
{
    return Null;
}

bool TObjectProxyBase::SetBuiltinAttribute(const Stroka& key, const TYsonString& value)
{
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* acd = FindThisAcd();
    if (acd) {
        if (key == "inherit_acl") {
            ValidateNoTransaction();

            acd->SetInherit(ConvertTo<bool>(value));
            return true;
        }

        if (key == "acl") {
            ValidateNoTransaction();

            auto valueNode = ConvertToNode(value);
            TAccessControlList newAcl;
            Deserilize(newAcl, valueNode, securityManager);

            acd->ClearEntries();
            for (const auto& ace : newAcl.Entries) {
                acd->AddEntry(ace);
            }

            return true;
        }

        if (key == "owner") {
            ValidateNoTransaction();

            auto name = ConvertTo<Stroka>(value);
            auto* owner = securityManager->GetSubjectByNameOrThrow(name);
            auto* user = securityManager->GetAuthenticatedUser();
            auto* superusers = securityManager->GetSuperusersGroup();
            if (user != owner && user->RecursiveMemberOf().find(superusers) == user->RecursiveMemberOf().end()) {
                THROW_ERROR_EXCEPTION(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: can only set owner to self");
            }

            acd->SetOwner(owner);

            return true;
        }
    }
    return false;
}

bool TObjectProxyBase::RemoveBuiltinAttribute(const Stroka& /*key*/)
{
    return false;
}

void TObjectProxyBase::ValidateCustomAttributeUpdate(
    const Stroka& /*key*/,
    const TNullable<TYsonString>& /*oldValue*/,
    const TNullable<TYsonString>& /*newValue*/)
{ }

void TObjectProxyBase::GuardedValidateCustomAttributeUpdate(
    const Stroka& key,
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    try {
        if (newValue) {
            ValidateCustomAttributeLength(*newValue);
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

void TObjectProxyBase::ValidateCustomAttributeLength(const NYson::TYsonString& value)
{
    auto size = value.GetData().length();
    auto limit = Bootstrap_->GetConfig()->CypressManager->MaxAttributeSize;
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
    YCHECK(NHydra::HasMutationContext());
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

void TObjectProxyBase::ValidatePermission(EPermissionCheckScope scope, EPermission permission)
{
    YCHECK(scope == EPermissionCheckScope::This);
    ValidatePermission(Object_, permission);
}

void TObjectProxyBase::ValidatePermission(TObjectBase* object, EPermission permission)
{
    YCHECK(object);
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();
    securityManager->ValidatePermission(object, user, permission);
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
    return Bootstrap_->IsPrimaryMaster();
}

bool TObjectProxyBase::IsSecondaryMaster() const
{
    return Bootstrap_->IsSecondaryMaster();
}

void TObjectProxyBase::RequireLeader() const
{
    Bootstrap_->GetHydraFacade()->RequireLeader();
}

void TObjectProxyBase::PostToSecondaryMasters(IServiceContextPtr context)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToSecondaryMasters(
        TCrossCellMessage(Object_->GetId(), std::move(context)));
}

void TObjectProxyBase::PostToMasters(IServiceContextPtr context, const TCellTagList& cellTags)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMasters(
        TCrossCellMessage(Object_->GetId(), std::move(context)),
        cellTags);
}

void TObjectProxyBase::PostToMaster(IServiceContextPtr context, TCellTag cellTag)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(
        TCrossCellMessage(Object_->GetId(), std::move(context)),
        cellTag);
}

////////////////////////////////////////////////////////////////////////////////

TNontemplateNonversionedObjectProxyBase::TNontemplateNonversionedObjectProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TObjectBase* object)
    : TObjectProxyBase(bootstrap, metadata, object)
{ }

bool TNontemplateNonversionedObjectProxyBase::DoInvoke(const IServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    return TObjectProxyBase::DoInvoke(context);
}

void TNontemplateNonversionedObjectProxyBase::GetSelf(
    TReqGet* /*request*/,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);
    context->SetRequestInfo();

    response->set_value("#");
    context->Reply();
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

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (objectManager->GetObjectRefCounter(Object_) != 1) {
        THROW_ERROR_EXCEPTION("Object is in use");
    }

    objectManager->UnrefObject(Object_);

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

} // namespace NObjectServer
} // namespace NYT

