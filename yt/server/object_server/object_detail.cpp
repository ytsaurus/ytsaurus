#include "stdafx.h"
#include "object_detail.h"
#include "object_manager.h"
#include "object_service.h"
#include "attribute_set.h"
#include "private.h"

#include <core/misc/string.h>
#include <core/misc/enum.h>

#include <core/ytree/fluent.h>
#include <core/ytree/yson_string.h>
#include <core/ytree/exception_helpers.h>

#include <core/ypath/tokenizer.h>

#include <core/rpc/message.h>
#include <core/rpc/helpers.h>
#include <core/rpc/rpc.pb.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/election/cell_manager.h>

#include <server/election/election_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/config.h>
#include <server/cell_master/serialize.h>

#include <server/cypress_server/virtual.h>

#include <server/transaction_server/transaction.h>

#include <server/security_server/account.h>
#include <server/security_server/security_manager.h>
#include <server/security_server/acl.h>
#include <server/security_server/user.h>

#include <server/object_server/type_handler.h>
#include <server/object_server/object_manager.h>

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
        const auto* attributes = FindAttributes();
        std::vector<Stroka> keys;
        if (attributes) {
            for (const auto& pair : attributes->Attributes()) {
                // Attribute cannot be empty (i.e. deleted) in null transaction.
                YASSERT(pair.second);
                keys.push_back(pair.first);
            }
        }
        return keys;
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const override
    {
        const auto* attributes = FindAttributes();
        if (!attributes) {
            return Null;
        }
        auto it = attributes->Attributes().find(key);
        if (it == attributes->Attributes().end()) {
            return Null;
        }
        // Attribute cannot be empty (i.e. deleted) in null transaction.
        YASSERT(it->second);
        return it->second;
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        auto oldValue = FindYson(key);
        Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, value);

        auto* attributes = GetOrCreateAttributes();
        attributes->Attributes()[key] = value;
    }

    virtual bool Remove(const Stroka& key) override
    {
        auto oldValue = FindYson(key);
        Proxy_->GuardedValidateCustomAttributeUpdate(key, oldValue, Null);

        auto* attributes = FindAttributes();
        if (!attributes) {
            return false;
        }
        auto it = attributes->Attributes().find(key);
        if (it == attributes->Attributes().end()) {
            return false;
        }
        // Attribute cannot be empty (i.e. deleted) in null transaction.
        YASSERT(it->second);
        attributes->Attributes().erase(it);
        if (attributes->Attributes().empty()) {
            RemoveAttributes();
        }
        return true;
    }

private:
    TObjectProxyBase* const Proxy_;

    mutable bool HasCachedAttributes_ = false;
    mutable TAttributeSet* CachedAttributes_ = nullptr;


    TAttributeSet* FindAttributes() const
    {
        if (!HasCachedAttributes_) {
            auto objectManager = Proxy_->Bootstrap_->GetObjectManager();
            CachedAttributes_ = objectManager->FindAttributes(TVersionedObjectId(Proxy_->GetId()));
            HasCachedAttributes_ = true;
        }
        return CachedAttributes_;
    }

    TAttributeSet* GetOrCreateAttributes()
    {
        if (!CachedAttributes_) {
            auto objectManager = Proxy_->Bootstrap_->GetObjectManager();
            CachedAttributes_ = objectManager->GetOrCreateAttributes(TVersionedObjectId(Proxy_->GetId()));
            HasCachedAttributes_ = true;
        }
        return CachedAttributes_;
    }

    void RemoveAttributes()
    {
        auto objectManager = Proxy_->Bootstrap_->GetObjectManager();
        objectManager->RemoveAttributes(TVersionedObjectId(Proxy_->GetId()));
        HasCachedAttributes_ = false;
        CachedAttributes_ = nullptr;
    }

};

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TBootstrap* bootstrap,
    TObjectBase* object)
    : Bootstrap_(bootstrap)
    , Object_(object)
{
    YASSERT(Bootstrap_);
    YASSERT(Object_);
}

const TObjectId& TObjectProxyBase::GetId() const
{
    return Object_->GetId();
}

const IAttributeDictionary& TObjectProxyBase::Attributes() const
{
    return *const_cast<TObjectProxyBase*>(this)->GetCustomAttributes();
}

IAttributeDictionary* TObjectProxyBase::MutableAttributes()
{
    return GetCustomAttributes();
}

DEFINE_YPATH_SERVICE_METHOD(TObjectProxyBase, GetBasicAttributes)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    ToProto(response->mutable_id(), GetId());
    response->set_type(static_cast<int>(Object_->GetType()));

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

    auto securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetUserByNameOrThrow(userName);

    auto result = securityManager->CheckPermission(Object_, user, permission);

    response->set_action(static_cast<int>(result.Action));
    if (result.Object) {
        ToProto(response->mutable_object_id(), result.Object->GetId());
    }
    if (result.Subject) {
        response->set_subject(result.Subject->GetName());
    }

    context->SetResponseInfo("Action: %v, Object: %v, Subject: %v",
        permission,
        result.Object ? ToString(result.Object->GetId()) : "<Null>",
        result.Subject ? ToString(result.Subject->GetId()) : "<Null>");
    context->Reply();
}

IYPathService::TResolveResult TObjectProxyBase::Resolve(const TYPath& path, IServiceContextPtr context)
{
    if (IsFollower() && !IsMutating() && IsLeaderReadRequired()) {
        throw TLeaderFallbackException();
    }
    return TYPathServiceBase::Resolve(path, context);
}

void TObjectProxyBase::Invoke(IServiceContextPtr context)
{
    const auto& requestHeader = context->RequestHeader();

    // Validate that mutating requests are only being invoked inside mutations or recovery.
    const auto& ypathExt = requestHeader.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    YCHECK(!ypathExt.mutating() ||
           hydraManager->IsMutating() ||
           hydraManager->IsRecovery());

    auto securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto objectManager = Bootstrap_->GetObjectManager();
    if (requestHeader.HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
        auto prerequiesitesExt = requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        objectManager->ValidatePrerequisites(prerequiesitesExt);
    }

    auto objectId = GetVersionedId();
    const auto& Logger = ObjectServerLogger;
    LOG_DEBUG_UNLESS(IsRecovery(), "Invoke: %v:%v %v (ObjectId: %v, Mutating: %v, User: %v, Leader: %v)",
        context->GetService(),
        context->GetMethod(),
        GetRequestYPath(context),
        objectId,
        ypathExt.mutating(),
        user->GetName(),
        hydraManager->IsLeader());

    NProfiling::TTagIdList tagIds;
    tagIds.push_back(objectManager->GetTypeTagId(TypeFromId(objectId.ObjectId)));
    tagIds.push_back(objectManager->GetMethodTagId(context->GetMethod()));
    auto& Profiler = objectManager->GetProfiler();
    PROFILE_TIMING ("/request_time", tagIds) {
        TSupportsAttributes::Invoke(std::move(context));
    }
}

void TObjectProxyBase::SerializeAttributes(
    IYsonConsumer* consumer,
    const TAttributeFilter& filter,
    bool sortKeys)
{
    if (filter.Mode == EAttributeFilterMode::None)
        return;

    if (filter.Mode == EAttributeFilterMode::MatchingOnly && filter.Keys.empty())
        return;

    class TAttributesConsumer
        : public IYsonConsumer
    {
    public:
        explicit TAttributesConsumer(IYsonConsumer* underlyingConsumer)
            : UnderlyingConsumer_(underlyingConsumer)
            , HasAttributes_(false)
        { }

        ~TAttributesConsumer()
        {
            if (HasAttributes_) {
                UnderlyingConsumer_->OnEndAttributes();
            }
        }

        virtual void OnStringScalar(const TStringBuf& value) override
        {
            UnderlyingConsumer_->OnStringScalar(value);
        }

        virtual void OnInt64Scalar(i64 value) override
        {
            UnderlyingConsumer_->OnInt64Scalar(value);
        }

        virtual void OnUint64Scalar(ui64 value) override
        {
            UnderlyingConsumer_->OnUint64Scalar(value);
        }

        virtual void OnDoubleScalar(double value) override
        {
            UnderlyingConsumer_->OnDoubleScalar(value);
        }

        virtual void OnBooleanScalar(bool value) override
        {
            UnderlyingConsumer_->OnBooleanScalar(value);
        }

        virtual void OnEntity() override
        {
            UnderlyingConsumer_->OnEntity();
        }

        virtual void OnBeginList() override
        {
            UnderlyingConsumer_->OnBeginList();
        }

        virtual void OnListItem() override
        {
            UnderlyingConsumer_->OnListItem();
        }

        virtual void OnEndList() override
        {
            UnderlyingConsumer_->OnEndList();
        }

        virtual void OnBeginMap() override
        {
            UnderlyingConsumer_->OnBeginMap();
        }

        virtual void OnKeyedItem(const TStringBuf& key) override
        {
            if (!HasAttributes_) {
                UnderlyingConsumer_->OnBeginAttributes();
                HasAttributes_ = true;
            }
            UnderlyingConsumer_->OnKeyedItem(key);
        }

        virtual void OnEndMap() override
        {
            UnderlyingConsumer_->OnEndMap();
        }

        virtual void OnBeginAttributes() override
        {
            UnderlyingConsumer_->OnBeginAttributes();
        }

        virtual void OnEndAttributes() override
        {
            UnderlyingConsumer_->OnEndAttributes();
        }

        virtual void OnRaw(const TStringBuf& yson, EYsonType type) override
        {
            UnderlyingConsumer_->OnRaw(yson, type);
        }

    private:
        IYsonConsumer* const UnderlyingConsumer_;
        bool HasAttributes_;

    };

    class TAttributeValueConsumer
        : public IYsonConsumer
    {
    public:
        TAttributeValueConsumer(IYsonConsumer* underlyingConsumer, const Stroka& key)
            : UnderlyingConsumer_(underlyingConsumer)
            , Key_(key)
            , Empty_(true)
        { }

        virtual void OnStringScalar(const TStringBuf& value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnStringScalar(value);
        }

        virtual void OnInt64Scalar(i64 value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnInt64Scalar(value);
        }

        virtual void OnUint64Scalar(ui64 value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnUint64Scalar(value);
        }

        virtual void OnDoubleScalar(double value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnDoubleScalar(value);
        }

        virtual void OnBooleanScalar(bool value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnBooleanScalar(value);
        }

        virtual void OnEntity() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnEntity();
        }

        virtual void OnBeginList() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnBeginList();
        }

        virtual void OnListItem() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnListItem();
        }

        virtual void OnEndList() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnEndList();
        }

        virtual void OnBeginMap() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnBeginMap();
        }

        virtual void OnKeyedItem(const TStringBuf& key) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnKeyedItem(key);
        }

        virtual void OnEndMap() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnEndMap();
        }

        virtual void OnBeginAttributes() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnBeginAttributes();
        }

        virtual void OnEndAttributes() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnEndAttributes();
        }

        virtual void OnRaw(const TStringBuf& yson, EYsonType type) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer_->OnRaw(yson, type);
        }

    private:
        IYsonConsumer* const UnderlyingConsumer_;
        const Stroka Key_;
        bool Empty_;


        void ProduceKeyIfNeeded()
        {
            if (Empty_) {
                UnderlyingConsumer_->OnKeyedItem(Key_);
                Empty_ = false;
            }
        }

    };

    TAttributesConsumer attributesConsumer(consumer);

    const auto& customAttributes = Attributes();

    switch (filter.Mode) {
        case EAttributeFilterMode::All: {
            std::vector<ISystemAttributeProvider::TAttributeInfo> builtinAttributes;
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
                    [] (const ISystemAttributeProvider::TAttributeInfo& lhs, const ISystemAttributeProvider::TAttributeInfo& rhs) {
                        return lhs.Key < rhs.Key;
                    });
            }

            for (const auto& key : userKeys) {
                attributesConsumer.OnKeyedItem(key);
                attributesConsumer.OnRaw(customAttributes.GetYson(key).Data(), EYsonType::Node);
            }

            for (const auto& attribute : builtinAttributes) {
                if (attribute.IsPresent){
                    attributesConsumer.OnKeyedItem(attribute.Key);
                    if (attribute.IsOpaque) {
                        attributesConsumer.OnEntity();
                    } else {
                        YCHECK(GetBuiltinAttribute(attribute.Key, &attributesConsumer));
                    }
                }
            }
            break;
        }

        case EAttributeFilterMode::MatchingOnly: {
            auto keys = filter.Keys;
            
            if (sortKeys) {
                std::sort(keys.begin(), keys.end());
            }

            for (const auto& key : keys) {
                TAttributeValueConsumer attributeValueConsumer(&attributesConsumer, key);
                if (!GetBuiltinAttribute(key, &attributeValueConsumer)) {
                    auto value = customAttributes.FindYson(key);
                    if (value) {
                        attributeValueConsumer.OnRaw(value->Data(), EYsonType::Node);
                    }
                }
            }

            break;
        }

        default:
            YUNREACHABLE();
    }
}

bool TObjectProxyBase::DoInvoke(IServiceContextPtr context)
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

void TObjectProxyBase::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    auto* acd = FindThisAcd();
    bool hasAcd = acd;
    bool hasOwner = acd && acd->GetOwner();

    attributes->push_back("id");
    attributes->push_back("type");
    attributes->push_back("builtin");
    attributes->push_back("ref_counter");
    attributes->push_back("weak_ref_counter");
    attributes->push_back(TAttributeInfo("supported_permissions", true, true));
    attributes->push_back(TAttributeInfo("inherit_acl", hasAcd, false));
    attributes->push_back(TAttributeInfo("acl", hasAcd, true));
    attributes->push_back(TAttributeInfo("owner", hasOwner, false));
    attributes->push_back(TAttributeInfo("effective_acl", true, true));
}

bool TObjectProxyBase::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    auto objectManager = Bootstrap_->GetObjectManager();
    auto securityManager = Bootstrap_->GetSecurityManager();

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
            .Value(Object_->GetObjectRefCounter());
        return true;
    }

    if (key == "weak_ref_counter") {
        BuildYsonFluently(consumer)
            .Value(Object_->GetObjectWeakRefCounter());
        return true;
    }

    if (key == "supported_permissions") {
        auto handler = objectManager->GetHandler(Object_);
        auto permissions = handler->GetSupportedPermissions();
        BuildYsonFluently(consumer)
            .Value(TEnumTraits<EPermissionSet>::Decompose(permissions));
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

TFuture<void> TObjectProxyBase::GetBuiltinAttributeAsync(const Stroka& key, IYsonConsumer* /*consumer*/)
{
    return Null;
}

bool TObjectProxyBase::SetBuiltinAttribute(const Stroka& key, const TYsonString& value)
{
    auto securityManager = Bootstrap_->GetSecurityManager();
    auto* acd = FindThisAcd();
    if (acd) {
        if (key == "inherit_acl") {
            ValidateNoTransaction();
            ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

            acd->SetInherit(ConvertTo<bool>(value));
            return true;
        }

        if (key == "acl") {
            ValidateNoTransaction();
            ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

            auto supportedPermissions = securityManager->GetSupportedPermissions(Object_);
            auto valueNode = ConvertToNode(value);
            TAccessControlList newAcl;
            Deserilize(newAcl, supportedPermissions, valueNode, securityManager);

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
            if (user != securityManager->GetRootUser() && user != owner) {
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

TObjectBase* TObjectProxyBase::GetSchema(EObjectType type)
{
    auto objectManager = Bootstrap_->GetObjectManager();
    return objectManager->GetSchema(type);
}

TObjectBase* TObjectProxyBase::GetThisSchema()
{
    return GetSchema(Object_->GetType());
}

void TObjectProxyBase::DeclareMutating()
{
    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    YCHECK(hydraManager->IsMutating());
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
    auto securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();
    securityManager->ValidatePermission(object, user, permission);
}

bool TObjectProxyBase::IsRecovery() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

bool TObjectProxyBase::IsMutating() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsMutating();
}

bool TObjectProxyBase::IsLeader() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsLeader();
}

bool TObjectProxyBase::IsFollower() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsFollower();
}

bool TObjectProxyBase::IsLeaderReadRequired() const
{
    return false;
}

bool TObjectProxyBase::IsLoggingEnabled() const
{
    return !IsRecovery();
}

NLog::TLogger TObjectProxyBase::CreateLogger() const
{
    return ObjectServerLogger;
}

////////////////////////////////////////////////////////////////////////////////

TNontemplateNonversionedObjectProxyBase::TNontemplateNonversionedObjectProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectBase* object)
    : TObjectProxyBase(bootstrap, object)
{ }

bool TNontemplateNonversionedObjectProxyBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    return TObjectProxyBase::DoInvoke(context);
}

void TNontemplateNonversionedObjectProxyBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    UNUSED(request);

    response->set_value("#");
    context->Reply();
}

void TNontemplateNonversionedObjectProxyBase::ValidateRemoval()
{
    THROW_ERROR_EXCEPTION("Object cannot be removed explicitly");
}

void TNontemplateNonversionedObjectProxyBase::RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemovePtr context)
{
    UNUSED(request);
    UNUSED(response);

    ValidateRemoval();

    if (Object_->GetObjectRefCounter() != 1) {
        THROW_ERROR_EXCEPTION("Object is in use");
    }

    auto objectManager = Bootstrap_->GetObjectManager();
    objectManager->UnrefObject(Object_);

    context->Reply();
}

TVersionedObjectId TNontemplateNonversionedObjectProxyBase::GetVersionedId() const
{
    return TVersionedObjectId(Object_->GetId());
}

TAccessControlDescriptor* TNontemplateNonversionedObjectProxyBase::FindThisAcd()
{
    auto securityManager = Bootstrap_->GetSecurityManager();
    return securityManager->FindAcd(Object_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

