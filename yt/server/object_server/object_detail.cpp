#include "stdafx.h"
#include "object_detail.h"
#include "object_manager.h"
#include "object_service.h"
#include "attribute_set.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/yson_string.h>

#include <ytlib/ypath/tokenizer.h>

#include <ytlib/rpc/message.h>
#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/meta_state_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>
#include <server/cell_master/config.h>
#include <server/cell_master/serialization_context.h>

#include <server/cypress_server/virtual.h>

#include <server/transaction_server/transaction.h>

#include <server/security_server/account.h>

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
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TUnversionedObjectBase::TUnversionedObjectBase(const TObjectId& id)
    : TObjectBase(id)
{ }

////////////////////////////////////////////////////////////////////////////////

TStagedObject::TStagedObject()
    : StagingTransaction_(NULL)
    , StagingAccount_(NULL)
{ }

void TStagedObject::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    SaveObjectRef(output, StagingTransaction_);
    SaveObjectRef(output, StagingAccount_);
}

void TStagedObject::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    LoadObjectRef(input, StagingTransaction_, context);
    LoadObjectRef(input, StagingAccount_, context);
}

bool TStagedObject::IsStaged() const
{
    return StagingTransaction_ && StagingAccount_;
}

////////////////////////////////////////////////////////////////////////////////

TUserAttributeDictionary::TUserAttributeDictionary(
    TObjectManagerPtr objectManager,
    const TObjectId& objectId)
    : ObjectManager(std::move(objectManager))
    , ObjectId(objectId)
{ }

std::vector<Stroka> TUserAttributeDictionary::List() const
{
    std::vector<Stroka> keys;
    const auto* attributeSet = ObjectManager->FindAttributes(TVersionedObjectId(ObjectId));
    if (attributeSet) {
        FOREACH (const auto& pair, attributeSet->Attributes()) {
            // Attribute cannot be empty (i.e. deleted) in null transaction.
            YASSERT(pair.second);
            keys.push_back(pair.first);
        }
    }
    return keys;
}

TNullable<TYsonString> TUserAttributeDictionary::FindYson(const Stroka& key) const
{
    const auto* attributeSet = ObjectManager->FindAttributes(TVersionedObjectId(ObjectId));
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

void TUserAttributeDictionary::SetYson(
    const Stroka& key,
    const NYTree::TYsonString& value)
{
    auto* attributeSet = ObjectManager->FindAttributes(TVersionedObjectId(ObjectId));
    if (!attributeSet) {
        attributeSet = ObjectManager->CreateAttributes(TVersionedObjectId(ObjectId));
    }
    attributeSet->Attributes()[key] = value;
}

bool TUserAttributeDictionary::Remove(const Stroka& key)
{
    auto* attributeSet = ObjectManager->FindAttributes(TVersionedObjectId(ObjectId));
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
        ObjectManager->RemoveAttributes(TVersionedObjectId(ObjectId));
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TBootstrap* bootstrap,
    TObjectBase* object)
    : Bootstrap(bootstrap)
    , Object(object)
{
    YASSERT(bootstrap);
    // TODO(babenko): special handling for null transaction
    //YASSERT(object);
}

TObjectProxyBase::~TObjectProxyBase()
{ }

const TObjectId& TObjectProxyBase::GetId() const
{
    // TODO(babenko): special handling for null transaction
    if (!Object) {
        return NullObjectId;
    }
    return Object->GetId();
}

IAttributeDictionary& TObjectProxyBase::Attributes()
{
    return *GetUserAttributes();
}

const IAttributeDictionary& TObjectProxyBase::Attributes() const
{
    return *const_cast<TObjectProxyBase*>(this)->GetUserAttributes();
}

DEFINE_RPC_SERVICE_METHOD(TObjectProxyBase, GetId)
{
    context->SetRequestInfo("");
    *response->mutable_object_id() = GetId().ToProto();
    context->Reply();
}

void TObjectProxyBase::Invoke(IServiceContextPtr context)
{
    Bootstrap->GetObjectManager()->ExecuteVerb(
        GetVersionedId(),
        IsWriteRequest(context),
        context,
        BIND(&TObjectProxyBase::GuardedInvoke, MakeStrong(this)));
}

void TObjectProxyBase::SerializeAttributes(
    IYsonConsumer* consumer,
    const TAttributeFilter& filter) const
{
    if (filter.Mode == EAttributeFilterMode::None ||
        filter.Mode == EAttributeFilterMode::MatchingOnly && filter.Keys.empty())
        return;

    const auto& userAttributes = Attributes();

    auto userKeys = userAttributes.List();

    std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
    ListSystemAttributes(&systemAttributes);

    yhash_set<Stroka> matchingKeys(filter.Keys.begin(), filter.Keys.end());

    bool seenMatching = false;

    FOREACH (const auto& key, userKeys) {
        if (filter.Mode == EAttributeFilterMode::All || matchingKeys.find(key) != matchingKeys.end()) {
            if (!seenMatching) {
                consumer->OnBeginAttributes();
                seenMatching = true;
            }
            consumer->OnKeyedItem(key);
            consumer->OnRaw(userAttributes.GetYson(key).Data(), EYsonType::Node);
        }
    }

    FOREACH (const auto& attribute, systemAttributes) {
        if (attribute.IsPresent &&
            (filter.Mode == EAttributeFilterMode::All || matchingKeys.find(attribute.Key) != matchingKeys.end()))
        {
            if (!seenMatching) {
                consumer->OnBeginAttributes();
                seenMatching = true;
            }
            consumer->OnKeyedItem(attribute.Key);
            if (attribute.IsOpaque) {
                consumer->OnEntity();
            } else {
                YCHECK(GetSystemAttribute(attribute.Key, consumer));
            }
        }
    }

    if (seenMatching) {
        consumer->OnEndAttributes();
    }
}

void TObjectProxyBase::GuardedInvoke(IServiceContextPtr context)
{
    try {
        DoInvoke(context);
    } catch (const TNotALeaderException&) {
        ForwardToLeader(context);
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }
}

void TObjectProxyBase::ForwardToLeader(IServiceContextPtr context)
{
    auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();
    auto epochContext = metaStateManager->GetEpochContext();

    LOG_DEBUG("Forwarding request to leader");

    auto cellManager = metaStateManager->GetCellManager();
    auto channel = cellManager->GetMasterChannel(epochContext->LeaderId);

    // Update request path to include the current object id and transaction id.
    auto requestMessage = context->GetRequestMessage();
    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    auto versionedId = GetVersionedId();
    requestHeader.set_path(FromObjectId(versionedId.ObjectId) + requestHeader.path());
    SetTransactionId(&requestHeader, versionedId.TransactionId);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    TObjectServiceProxy proxy(channel);
    // TODO(babenko): use proper timeout
    proxy.SetDefaultTimeout(Bootstrap->GetConfig()->MetaState->RpcTimeout);
    proxy
        .Execute(updatedRequestMessage)
        .Subscribe(BIND(&TObjectProxyBase::OnLeaderResponse, MakeStrong(this), context));
}

void TObjectProxyBase::OnLeaderResponse(IServiceContextPtr context, NBus::IMessagePtr responseMessage)
{
    NRpc::NProto::TResponseHeader responseHeader;
    YCHECK(ParseResponseHeader(responseMessage, &responseHeader));
    auto error = FromProto(responseHeader.error());
    LOG_DEBUG(error, "Received response for forwarded request");
    context->Reply(responseMessage);
}

void TObjectProxyBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    DISPATCH_YPATH_SERVICE_METHOD(List);
    DISPATCH_YPATH_SERVICE_METHOD(Set);
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    TYPathServiceBase::DoInvoke(context);
}

bool TObjectProxyBase::IsWriteRequest(IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Set);
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Remove);
    return TYPathServiceBase::IsWriteRequest(context);
}

IAttributeDictionary* TObjectProxyBase::GetUserAttributes()
{
    if (!UserAttributes.Get()) {
        UserAttributes = DoCreateUserAttributes();
    }
    return UserAttributes.Get();
}

ISystemAttributeProvider* TObjectProxyBase::GetSystemAttributeProvider()
{
    return this;
}

TAutoPtr<IAttributeDictionary> TObjectProxyBase::DoCreateUserAttributes()
{
    return new TUserAttributeDictionary(
        Bootstrap->GetObjectManager(),
        GetId());
}

void TObjectProxyBase::ListSystemAttributes(std::vector<TAttributeInfo>* names) const
{
    names->push_back("id");
    names->push_back("type");
    names->push_back("ref_counter");
}

bool TObjectProxyBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const
{
    if (key == "id") {
        BuildYsonFluently(consumer)
            .Value(GetId().ToString());
        return true;
    }

    if (key == "type") {
        BuildYsonFluently(consumer)
            .Value(CamelCaseToUnderscoreCase(TypeFromId(GetId()).ToString()));
        return true;
    }

    if (key == "ref_counter") {
        BuildYsonFluently(consumer)
            .Value(Object->GetObjectRefCounter());
        return true;
    }

    return false;
}

TAsyncError TObjectProxyBase::GetSystemAttributeAsync(const Stroka& key, IYsonConsumer* consumer) const
{
    return Null;
}

bool TObjectProxyBase::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    UNUSED(key);
    UNUSED(value);
    return false;
}

TVersionedObjectId TObjectProxyBase::GetVersionedId() const
{
    // TODO(babenko): special hanlding for null transaction
    if (!Object) {
        return TVersionedObjectId();
    }
    return TVersionedObjectId(Object->GetId());
}

bool TObjectProxyBase::IsRecovery() const
{
    return Bootstrap->GetMetaStateFacade()->GetManager()->IsRecovery();
}

bool TObjectProxyBase::IsLeader() const
{
    return Bootstrap->GetMetaStateFacade()->GetManager()->IsLeader();
}

void TObjectProxyBase::ValidateActiveLeader() const
{
    Bootstrap->GetMetaStateFacade()->ValidateActiveLeader();
}

////////////////////////////////////////////////////////////////////////////////

TNonversionedObjectProxyNontemplateBase::TNonversionedObjectProxyNontemplateBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectBase* object)
    : TObjectProxyBase(bootstrap, object)
{ }

bool TNonversionedObjectProxyNontemplateBase::IsWriteRequest(IServiceContextPtr context) const 
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(Remove);
    return TObjectProxyBase::IsWriteRequest(context);
}

void TNonversionedObjectProxyNontemplateBase::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Remove);
    TObjectProxyBase::DoInvoke(context);
}

void TNonversionedObjectProxyNontemplateBase::GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
{
    UNUSED(request);

    response->set_value("#");
    context->Reply();
}

void TNonversionedObjectProxyNontemplateBase::ValidateRemoval()
{
    THROW_ERROR_EXCEPTION("Object cannot be removed explicitly");
}

DEFINE_RPC_SERVICE_METHOD(TNonversionedObjectProxyNontemplateBase, Remove)
{
    ValidateRemoval();

    if (Object->GetObjectRefCounter() != 1) {
        THROW_ERROR_EXCEPTION("Object is in use");
    }

    auto objectManager = Bootstrap->GetObjectManager();
    objectManager->UnrefObject(Object);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

