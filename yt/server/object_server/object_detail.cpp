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
#include <core/rpc/rpc.pb.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/rpc_helpers.h>

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

#include <stdexcept>

namespace NYT {
namespace NObjectServer {

using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NHydra;
using namespace NSecurityClient;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

TStagedObject::TStagedObject()
    : StagingTransaction_(nullptr)
    , StagingAccount_(nullptr)
{ }

void TStagedObject::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, StagingTransaction_);
    Save(context, StagingAccount_);
}

void TStagedObject::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, StagingTransaction_);
    Load(context, StagingAccount_);
}

bool TStagedObject::IsStaged() const
{
    return StagingTransaction_ && StagingAccount_;
}

////////////////////////////////////////////////////////////////////////////////

class TObjectProxyBase::TCustomAttributeDictionary
    : public IAttributeDictionary
{
public:
    explicit TCustomAttributeDictionary(TObjectProxyBase* proxy)
        : Proxy(proxy)
    { }

    // IAttributeDictionary members
    virtual std::vector<Stroka> List() const override
    {
        const auto& id = Proxy->GetId();
        auto objectManager = Proxy->Bootstrap->GetObjectManager();
        const auto* attributeSet = objectManager->FindAttributes(TVersionedObjectId(id));
        std::vector<Stroka> keys;
        if (attributeSet) {
            for (const auto& pair : attributeSet->Attributes()) {
                // Attribute cannot be empty (i.e. deleted) in null transaction.
                YASSERT(pair.second);
                keys.push_back(pair.first);
            }
        }
        return keys;
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const override
    {
        const auto& id = Proxy->GetId();
        auto objectManager = Proxy->Bootstrap->GetObjectManager();
        const auto* attributeSet = objectManager->FindAttributes(TVersionedObjectId(id));
        if (!attributeSet) {
            return Null;
        }
        auto it = attributeSet->Attributes().find(key);
        if (it == attributeSet->Attributes().end()) {
            return Null;
        }
        // Attribute cannot be empty (i.e. deleted) in null transaction.
        YASSERT(it->second);
        return it->second;
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        auto oldValue = FindYson(key);
        Proxy->GuardedValidateCustomAttributeUpdate(key, oldValue, value);

        const auto& id = Proxy->GetId();
        auto objectManager = Proxy->Bootstrap->GetObjectManager();
        auto* attributeSet = objectManager->GetOrCreateAttributes(TVersionedObjectId(id));
        attributeSet->Attributes()[key] = value;
    }

    virtual bool Remove(const Stroka& key) override
    {
        auto oldValue = FindYson(key);
        Proxy->GuardedValidateCustomAttributeUpdate(key, oldValue, Null);

        const auto& id = Proxy->GetId();
        auto objectManager = Proxy->Bootstrap->GetObjectManager();
        auto* attributeSet = objectManager->FindAttributes(TVersionedObjectId(id));
        if (!attributeSet) {
            return false;
        }
        auto it = attributeSet->Attributes().find(key);
        if (it == attributeSet->Attributes().end()) {
            return false;
        }
        // Attribute cannot be empty (i.e. deleted) in null transaction.
        YASSERT(it->second);
        attributeSet->Attributes().erase(it);
        if (attributeSet->Attributes().empty()) {
            objectManager->RemoveAttributes(TVersionedObjectId(id));
        }
        return true;
    }

private:
    TObjectProxyBase* Proxy;

};

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TBootstrap* bootstrap,
    TObjectBase* object)
    : Bootstrap(bootstrap)
    , Object(object)
{
    YASSERT(bootstrap);
    YASSERT(object);
}

TObjectProxyBase::~TObjectProxyBase()
{ }

const TObjectId& TObjectProxyBase::GetId() const
{
    return Object->GetId();
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
    response->set_type(Object->GetType());

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TObjectProxyBase, CheckPermission)
{
    DeclareNonMutating();

    auto userName = request->user();
    auto permission = EPermission(request->permission());
    context->SetRequestInfo("User: %s, Permission: %s",
        ~userName,
        ~ToString(permission));

    auto securityManager = Bootstrap->GetSecurityManager();
    auto* user = securityManager->GetUserByNameOrThrow(userName);

    auto result = securityManager->CheckPermission(Object, user, permission);

    response->set_action(result.Action);
    if (result.Object) {
        ToProto(response->mutable_object_id(), result.Object->GetId());
    }
    if (result.Subject) {
        response->set_subject(result.Subject->GetName());
    }

    context->SetResponseInfo("Action: %s, Object: %s, Subject: %s",
        ~ToString(permission),
        result.Object ? ~ToString(result.Object->GetId()) : "<Null>",
        result.Subject ? ~ToString(result.Subject->GetId()) : "<Null>");
    context->Reply();
}

void TObjectProxyBase::Invoke(IServiceContextPtr context)
{
    Bootstrap
        ->GetObjectManager()
        ->InterceptProxyInvocation(this, std::move(context));
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
            : UnderlyingConsumer(underlyingConsumer)
            , HasAttributes(false)
        { }

        ~TAttributesConsumer()
        {
            if (HasAttributes) {
                UnderlyingConsumer->OnEndAttributes();
            }
        }

        virtual void OnStringScalar(const TStringBuf& value) override
        {
            UnderlyingConsumer->OnStringScalar(value);
        }

        virtual void OnInt64Scalar(i64 value) override
        {
            UnderlyingConsumer->OnInt64Scalar(value);
        }

        virtual void OnDoubleScalar(double value) override
        {
            UnderlyingConsumer->OnDoubleScalar(value);
        }

        virtual void OnBooleanScalar(bool value) override
        {
            UnderlyingConsumer->OnBooleanScalar(value);
        }

        virtual void OnEntity() override
        {
            UnderlyingConsumer->OnEntity();
        }

        virtual void OnBeginList() override
        {
            UnderlyingConsumer->OnBeginList();
        }

        virtual void OnListItem() override
        {
            UnderlyingConsumer->OnListItem();
        }

        virtual void OnEndList() override
        {
            UnderlyingConsumer->OnEndList();
        }

        virtual void OnBeginMap() override
        {
            UnderlyingConsumer->OnBeginMap();
        }

        virtual void OnKeyedItem(const TStringBuf& key) override
        {
            if (!HasAttributes) {
                UnderlyingConsumer->OnBeginAttributes();
                HasAttributes = true;
            }
            UnderlyingConsumer->OnKeyedItem(key);
        }

        virtual void OnEndMap() override
        {
            UnderlyingConsumer->OnEndMap();
        }

        virtual void OnBeginAttributes() override
        {
            UnderlyingConsumer->OnBeginAttributes();
        }

        virtual void OnEndAttributes() override
        {
            UnderlyingConsumer->OnEndAttributes();
        }

        virtual void OnRaw(const TStringBuf& yson, EYsonType type) override
        {
            UnderlyingConsumer->OnRaw(yson, type);
        }

    private:
        IYsonConsumer* UnderlyingConsumer;
        bool HasAttributes;

    };

    class TAttributeValueConsumer
        : public IYsonConsumer
    {
    public:
        TAttributeValueConsumer(IYsonConsumer* underlyingConsumer, const Stroka& key)
            : UnderlyingConsumer(underlyingConsumer)
            , Key(key)
            , Empty(true)
        { }

        virtual void OnStringScalar(const TStringBuf& value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnStringScalar(value);
        }

        virtual void OnInt64Scalar(i64 value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnInt64Scalar(value);
        }

        virtual void OnDoubleScalar(double value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnDoubleScalar(value);
        }

        virtual void OnBooleanScalar(bool value) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnBooleanScalar(value);
        }

        virtual void OnEntity() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnEntity();
        }

        virtual void OnBeginList() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnBeginList();
        }

        virtual void OnListItem() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnListItem();
        }

        virtual void OnEndList() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnEndList();
        }

        virtual void OnBeginMap() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnBeginMap();
        }

        virtual void OnKeyedItem(const TStringBuf& key) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnKeyedItem(key);
        }

        virtual void OnEndMap() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnEndMap();
        }

        virtual void OnBeginAttributes() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnBeginAttributes();
        }

        virtual void OnEndAttributes() override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnEndAttributes();
        }

        virtual void OnRaw(const TStringBuf& yson, EYsonType type) override
        {
            ProduceKeyIfNeeded();
            UnderlyingConsumer->OnRaw(yson, type);
        }

    private:
        IYsonConsumer* UnderlyingConsumer;
        Stroka Key;
        bool Empty;

        void ProduceKeyIfNeeded()
        {
            if (Empty) {
                UnderlyingConsumer->OnKeyedItem(Key);
                Empty = false;
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

void TObjectProxyBase::GuardedInvoke(IServiceContextPtr context)
{
    // Cf. TYPathServiceBase::GuardedInvoke
    TError error;
    try {
        BeforeInvoke(context);
        if (!DoInvoke(context)) {
            ThrowMethodNotSupported(context->GetMethod());
        }
    } catch (const TNotALeaderException&) {
        // TODO(babenko): currently broken
        ForwardToLeader(context);
        return;
    } catch (const std::exception& ex) {
        error = ex;
    }
    
    AfterInvoke(context);

    if (!error.IsOK()) {
        context->Reply(error);
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
    if (!CustomAttributes) {
        CustomAttributes = DoCreateCustomAttributes();
    }
    return CustomAttributes.get();
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
    auto objectManager = Bootstrap->GetObjectManager();
    auto securityManager = Bootstrap->GetSecurityManager();

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

    if (key == "ref_counter") {
        BuildYsonFluently(consumer)
            .Value(Object->GetObjectRefCounter());
        return true;
    }

    if (key == "weak_ref_counter") {
        BuildYsonFluently(consumer)
            .Value(Object->GetObjectWeakRefCounter());
        return true;
    }

    if (key == "supported_permissions") {
        auto handler = objectManager->GetHandler(Object);
        auto permissions = handler->GetSupportedPermissions();
        BuildYsonFluently(consumer)
            .Value(permissions.Decompose());
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
            .Value(securityManager->GetEffectiveAcl(Object));
        return true;
    }

    return false;
}

TAsyncError TObjectProxyBase::GetBuiltinAttributeAsync(const Stroka& key, IYsonConsumer* consumer)
{
    return Null;
}

bool TObjectProxyBase::SetBuiltinAttribute(const Stroka& key, const TYsonString& value)
{
    auto securityManager = Bootstrap->GetSecurityManager();
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

            auto supportedPermissions = securityManager->GetSupportedPermissions(Object);
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
    auto objectManager = Bootstrap->GetObjectManager();
    return objectManager->GetSchema(type);
}

TObjectBase* TObjectProxyBase::GetThisSchema()
{
    return GetSchema(Object->GetType());
}

void TObjectProxyBase::DeclareMutating()
{
    auto hydraManager = Bootstrap->GetHydraFacade()->GetHydraManager();
    YCHECK(hydraManager->IsMutating());
}

void TObjectProxyBase::DeclareNonMutating()
{
    auto hydraManager = Bootstrap->GetHydraFacade()->GetHydraManager();
    if (hydraManager->IsMutating()) {
        auto* mutationContext = hydraManager->GetMutationContext();
        mutationContext->SuppressMutation();
    }
}

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
    ValidatePermission(Object, permission);
}

void TObjectProxyBase::ValidatePermission(TObjectBase* object, EPermission permission)
{
    YCHECK(object);
    auto securityManager = Bootstrap->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();
    securityManager->ValidatePermission(object, user, permission);
}

bool TObjectProxyBase::IsRecovery() const
{
    return Bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

bool TObjectProxyBase::IsLeader() const
{
    return Bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader();
}

void TObjectProxyBase::ValidateActiveLeader() const
{
    Bootstrap->GetHydraFacade()->ValidateActiveLeader();
}

void TObjectProxyBase::ForwardToLeader(IServiceContextPtr context)
{
    auto hydraManager = Bootstrap->GetHydraFacade()->GetHydraManager();
    auto epochContext = hydraManager->GetAutomatonEpochContext();

    LOG_DEBUG("Forwarding request to leader");

    auto cellManager = Bootstrap->GetCellManager();
    auto channel = cellManager->GetPeerChannel(epochContext->LeaderId);

    // Update request path to include the current object id and transaction id.
    auto requestMessage = context->GetRequestMessage();
    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    auto versionedId = GetVersionedId();
    const auto& path = GetRequestYPath(requestHeader);
    SetRequestYPath(&requestHeader, FromObjectId(versionedId.ObjectId) + path);
    SetTransactionId(&requestHeader, versionedId.TransactionId);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    TObjectServiceProxy proxy(channel);
    // TODO(babenko): timeout?
    // TODO(babenko): prerequisite transactions?
    // TODO(babenko): authenticated user?
    proxy.SetDefaultTimeout(Bootstrap->GetConfig()->HydraManager->RpcTimeout);
    auto batchReq = proxy.ExecuteBatch();
    batchReq->AddRequestMessage(updatedRequestMessage);
    batchReq->Invoke().Subscribe(
        BIND(&TObjectProxyBase::OnLeaderResponse, MakeStrong(this), context));
}

void TObjectProxyBase::OnLeaderResponse(IServiceContextPtr context, TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    if (!batchRsp->IsOK()) {
        auto error = batchRsp->GetError();
        LOG_DEBUG(error, "Error forwarding request to leader");
        context->Reply(error);
        return;
    }

    auto responseMessage = batchRsp->GetResponseMessage(0);
    NRpc::NProto::TResponseHeader responseHeader;
    YCHECK(ParseResponseHeader(responseMessage, &responseHeader));
    auto error = FromProto<TError>(responseHeader.error());
    LOG_DEBUG(error, "Received response for forwarded request");
    context->Reply(responseMessage);
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

    if (Object->GetObjectRefCounter() != 1) {
        THROW_ERROR_EXCEPTION("Object is in use");
    }

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->UnrefObject(Object);

    context->Reply();
}

TVersionedObjectId TNontemplateNonversionedObjectProxyBase::GetVersionedId() const
{
    return TVersionedObjectId(Object->GetId());
}

TAccessControlDescriptor* TNontemplateNonversionedObjectProxyBase::FindThisAcd()
{
    auto securityManager = Bootstrap->GetSecurityManager();
    return securityManager->FindAcd(Object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

