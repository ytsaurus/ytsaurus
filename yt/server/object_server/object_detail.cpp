#include "stdafx.h"
#include "object_detail.h"
#include "object_manager.h"
#include "object_service.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/tokenizer.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>
#include <server/cell_master/config.h>

#include <ytlib/rpc/message.h>
#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/meta_state_manager.h>

#include <stdexcept>

namespace NYT {
namespace NObjectServer {

using namespace NRpc;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TObjectBase::TObjectBase()
    : RefCounter(0)
{ }

i32 TObjectBase::RefObject()
{
    YASSERT(RefCounter >= 0);
    return ++RefCounter;
}

i32 TObjectBase::UnrefObject()
{
    YASSERT(RefCounter > 0);
    return --RefCounter;
}

i32 TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

void TObjectBase::Save(TOutputStream* output) const
{
    ::Save(output, RefCounter);
}

void TObjectBase::Load(TInputStream* input)
{
    ::Load(input, RefCounter);
}

////////////////////////////////////////////////////////////////////////////////

TObjectWithIdBase::TObjectWithIdBase()
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectId& id)
    : Id_(id)
{ }

////////////////////////////////////////////////////////////////////////////////

class TLeaderFallbackException
    : public std::runtime_error
{
public:
    TLeaderFallbackException()
        : std::runtime_error("Not a leader")
    { }
};

////////////////////////////////////////////////////////////////////////////////

TUserAttributeDictionary::TUserAttributeDictionary(
    TObjectManagerPtr objectManager,
    const TObjectId& objectId)
    : ObjectManager(MoveRV(objectManager))
    , ObjectId(objectId)
{ }

std::vector<Stroka> TUserAttributeDictionary::List() const
{
    std::vector<Stroka> keys;
    const auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
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
    const auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
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
    auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
    if (!attributeSet) {
        attributeSet = ObjectManager->CreateAttributes(ObjectId);
    }
    attributeSet->Attributes()[key] = value;
}

bool TUserAttributeDictionary::Remove(const Stroka& key)
{
    auto* attributeSet = ObjectManager->FindAttributes(ObjectId);
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
        ObjectManager->RemoveAttributes(ObjectId);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TObjectProxyBase::TObjectProxyBase(
    TBootstrap* bootstrap,
    const TObjectId& id)
    : Bootstrap(bootstrap)
    , Id(id)
{ }

TObjectProxyBase::~TObjectProxyBase()
{ }

const TObjectId& TObjectProxyBase::GetId() const
{
    return Id;
}

IAttributeDictionary& TObjectProxyBase::Attributes()
{
    return CombinedAttributes();
}

const IAttributeDictionary& TObjectProxyBase::Attributes() const
{
    return CombinedAttributes();
}

DEFINE_RPC_SERVICE_METHOD(TObjectProxyBase, GetId)
{
    context->SetRequestInfo("AllowNonemptyPathSuffix: ",
        ~FormatBool(request->allow_nonempty_path_suffix()));

    if (!request->allow_nonempty_path_suffix()) {
        TTokenizer tokenizer(context->GetPath());
        if (tokenizer.ParseNext()) {
            THROW_ERROR_EXCEPTION("Unexpected path suffix %s", ~context->GetPath());
        }
    }

    *response->mutable_object_id() = Id.ToProto();
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

void TObjectProxyBase::GuardedInvoke(IServiceContextPtr context)
{
    try {
        DoInvoke(context);
    } catch (const TLeaderFallbackException&) {
        ForwardToLeader(context);
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }
}

void TObjectProxyBase::ForwardToLeader(IServiceContextPtr context)
{
    auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();
    auto epochContext = metaStateManager->GetEpochContext();

    LOG_DEBUG("Forwarding request to leader (RequestId: %s, LeaderId: %d)",
        ~context->GetRequestId().ToString(),
        epochContext->LeaderId);

    auto cellManager = metaStateManager->GetCellManager();
    auto channel = cellManager->GetMasterChannel(epochContext->LeaderId);

    // Update request path to include the current object id and transaction id.
    auto requestMessage = context->GetRequestMessage();
    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    auto versionedId = GetVersionedId();
    auto pathPrefix = WithTransaction(FromObjectId(versionedId.ObjectId), versionedId.TransactionId);
    requestHeader.set_path(pathPrefix + requestHeader.path());
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
    LOG_DEBUG("Received response for forwarded request (RequestId: %s)\n%s",
        ~context->GetRequestId().ToString(),
        ~ToString(error));
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
        Id);
}

void TObjectProxyBase::GetSystemAttributes(std::vector<TAttributeInfo>* names)
{
    names->push_back("id");
    names->push_back("type");
    names->push_back("ref_counter");
}

bool TObjectProxyBase::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    if (key == "id") {
        BuildYsonFluently(consumer)
            .Scalar(GetId().ToString());
        return true;
    }

    if (key == "type") {
        BuildYsonFluently(consumer)
            .Scalar(CamelCaseToUnderscoreCase(TypeFromId(Id).ToString()));
        return true;
    }

    if (key == "ref_counter") {
        BuildYsonFluently(consumer)
            .Scalar(Bootstrap->GetObjectManager()->GetObjectRefCounter(Id));
        return true;
    }

    return false;
}

bool TObjectProxyBase::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    UNUSED(key);
    UNUSED(value);

    return false;
}

TVersionedObjectId TObjectProxyBase::GetVersionedId() const
{
    return Id;
}

bool TObjectProxyBase::IsRecovery() const
{
    return Bootstrap->GetMetaStateFacade()->GetManager()->IsRecovery();
}

void TObjectProxyBase::ValidateLeaderStatus()
{
    auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();
    if (metaStateManager->GetStateStatus() != EPeerStatus::Leading) {
        throw TLeaderFallbackException();
    }
    if (!metaStateManager->HasActiveQuorum()) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "No active quorum");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

