#include "stdafx.h"
#include "object_manager.h"
#include "config.h"

#include <ytlib/cell_master/load_context.h>
#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/transaction.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/rpc/message.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/cell_master/bootstrap.h>

#include <util/digest/murmur.h>

namespace NYT {
namespace NObjectServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NProto;
using namespace NCypress;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ObjectServer");

////////////////////////////////////////////////////////////////////////////////

//! A wrapper that is used to postpone a reply until the change is committed by quorum.
class TObjectManager::TServiceContextWrapper
    : public IServiceContext
{
public:
    TServiceContextWrapper(IServiceContext* underlyingContext)
        : UnderlyingContext(underlyingContext)
        , Replied(false)
    { }

    virtual NBus::IMessage::TPtr GetRequestMessage() const
    {
        return UnderlyingContext->GetRequestMessage();
    }

    virtual const NRpc::TRequestId& GetRequestId() const
    {
        return UnderlyingContext->GetRequestId();
    }

    virtual const Stroka& GetPath() const
    {
        return UnderlyingContext->GetPath();
    }

    virtual const Stroka& GetVerb() const
    {
        return UnderlyingContext->GetVerb();
    }

    virtual bool IsOneWay() const
    {
        return UnderlyingContext->IsOneWay();
    }

    virtual bool IsReplied() const
    {
        return Replied;
    }

    virtual void Reply(const TError& error)
    {
        YASSERT(!Replied);
        Replied = true;
        Error = error;
    }

    void Flush()
    {
        YASSERT(Replied);
        UnderlyingContext->Reply(Error);
    }

    virtual TError GetError() const
    {
        return Error;
    }

    virtual TSharedRef GetRequestBody() const
    {
        return UnderlyingContext->GetRequestBody();
    }

    virtual void SetResponseBody(const TSharedRef& responseBody)
    {
        UnderlyingContext->SetResponseBody(responseBody);
    }

    virtual const yvector<TSharedRef>& RequestAttachments() const
    {
        return UnderlyingContext->RequestAttachments();
    }

    virtual yvector<TSharedRef>& ResponseAttachments()
    {
        return UnderlyingContext->ResponseAttachments();
    }

    virtual const IAttributeDictionary& RequestAttributes() const
    {
        return UnderlyingContext->RequestAttributes();
    }

    virtual IAttributeDictionary& ResponseAttributes()
    {
        return UnderlyingContext->ResponseAttributes();
    }

    virtual void SetRequestInfo(const Stroka& info)
    {
        UnderlyingContext->SetRequestInfo(info);
    }

    virtual Stroka GetRequestInfo() const
    {
        return UnderlyingContext->GetRequestInfo();
    }

    virtual void SetResponseInfo(const Stroka& info)
    {
        UnderlyingContext->SetResponseInfo(info);
    }

    virtual Stroka GetResponseInfo()
    {
        return UnderlyingContext->GetRequestInfo();
    }

    virtual IAction::TPtr Wrap(IAction::TPtr action) 
    {
        return UnderlyingContext->Wrap(action);
    }

private:
    IServiceContext::TPtr UnderlyingContext;
    TError Error;
    bool Replied;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    TRootService(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        if (path.empty()) {
            ythrow yexception() << "YPath cannot be empty";
        }

        auto currentPath = path;
        auto transactionId = NullTransactionId;
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();
        auto transactionManager = Bootstrap->GetTransactionManager();
        auto objectId = cypressManager->GetRootNodeId();

        if (!currentPath.empty() && currentPath.has_prefix(TransactionIdMarker)) {
            Stroka token;
            ChopTransactionIdToken(currentPath, &token, &currentPath);
            if (!TObjectId::FromString(token.substr(TransactionIdMarker.length()), &transactionId)) {
                ythrow yexception() << Sprintf("Error parsing transaction id (Value: %s)", ~token);
            }

            if (transactionId != NullTransactionId && !transactionManager->FindTransaction(transactionId)) {
                ythrow yexception() <<  Sprintf("No such transaction (TransactionId: %s)", ~transactionId.ToString());
            }
        }

        if (currentPath.has_prefix(RootMarker)) {
            currentPath = currentPath.substr(RootMarker.length());
            objectId = cypressManager->GetRootNodeId();
        } else if (currentPath.has_prefix(ObjectIdMarker)) {
            Stroka token;
            ChopYPathToken(currentPath, &token, &currentPath);
            if (!TObjectId::FromString(token.substr(ObjectIdMarker.length()), &objectId)) {
                ythrow yexception() << Sprintf("Error parsing object id (Value: %s)", ~token);
            }
        } else {
            ythrow yexception() << Sprintf("Invalid YPath syntax (Path: %s)", ~path);
        }

        auto proxy = objectManager->FindProxy(TVersionedObjectId(objectId, transactionId));
        if (!proxy) {
            ythrow yexception() << Sprintf("No such object (ObjectId: %s)", ~objectId.ToString());
        }

        return TResolveResult::There(~proxy, currentPath);
    }

    virtual void Invoke(IServiceContext* context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const
    {
        return NObjectServer::Logger.GetCategory();
    }

    virtual bool IsWriteRequest(IServiceContext* context) const
    {
        UNUSED(context);
        YUNREACHABLE();
    }

private:
    TBootstrap* Bootstrap;

    static void ChopTransactionIdToken(
        const TYPath& path,
        Stroka* token,
        TYPath* suffixPath)
    {
        size_t index = path.find_first_of("/#");
        if (index == TYPath::npos) {
            ythrow yexception() << Sprintf("YPath does not refer to any object (Path: %s)", ~path);
        }

        *token = path.substr(0, index);
        *suffixPath = path.substr(index);
    }

};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    TObjectManagerConfig* config,
    TBootstrap* bootstrap)
    : TMetaStatePart(
        ~bootstrap->GetMetaStateManager(),
        ~bootstrap->GetMetaState())
    , Config(config)
    , Bootstrap(bootstrap)
    , TypeToHandler(MaxObjectType)
    , TypeToCounter(MaxObjectType)
    , RootService(New<TRootService>(bootstrap))
{
    YASSERT(config);
    YASSERT(bootstrap);

    TLoadContext context(bootstrap);

    auto metaState = bootstrap->GetMetaState();
    metaState->RegisterLoader(
        "ObjectManager.Keys.1",
        FromMethod(&TObjectManager::LoadKeys, MakeStrong(this)));
    metaState->RegisterLoader(
        "ObjectManager.Values.1",
        FromMethod(&TObjectManager::LoadValues, MakeStrong(this), context));
    metaState->RegisterSaver(
        "ObjectManager.Keys.1",
        FromMethod(&TObjectManager::SaveKeys, MakeStrong(this)),
        ESavePhase::Keys);
    metaState->RegisterSaver(
        "ObjectManager.Values.1",
        FromMethod(&TObjectManager::SaveValues, MakeStrong(this)),
        ESavePhase::Values);

    metaState->RegisterPart(this);

    RegisterMethod(this, &TObjectManager::ReplayVerb);

    LOG_INFO("Object Manager initialized (CellId: %d)",
        static_cast<int>(config->CellId));
}

IYPathService* TObjectManager::GetRootService()
{
    return ~RootService;
}

void TObjectManager::RegisterHandler(IObjectTypeHandler* handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YASSERT(handler);
    int typeValue = handler->GetType().ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    YASSERT(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;
    TypeToCounter[typeValue] = TIdGenerator<ui64>();
}

IObjectTypeHandler* TObjectManager::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
    }

    return ~TypeToHandler[typeValue];
}

IObjectTypeHandler* TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = FindHandler(type);
    YASSERT(handler);
    return handler;
}

IObjectTypeHandler* TObjectManager::GetHandler(const TObjectId& id) const
{
    return GetHandler(TypeFromId(id));
}

TCellId TObjectManager::GetCellId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config->CellId;
}

TObjectId TObjectManager::GenerateId(EObjectType type)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);

    ui64 counter = TypeToCounter[typeValue].Next();
    auto cellId = GetCellId();

    char data[12];
    *reinterpret_cast<ui64*>(&data[ 0]) = counter;
    *reinterpret_cast<ui16*>(&data[ 8]) = typeValue;
    *reinterpret_cast<ui16*>(&data[10]) = cellId;
    ui32 hash = MurmurHash<ui32>(&data, sizeof (data), 0);

    TObjectId id(
        hash,
        (cellId << 16) + type.ToValue(),
        counter & 0xffffffff,
        counter >> 32);

    LOG_DEBUG_IF(!IsRecovery(), "Object id generated (Type: %s, Id: %s)",
        ~type.ToString(),
        ~id.ToString());

    return id;
}

void TObjectManager::RefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = GetHandler(id)->RefObject(id);
    LOG_DEBUG_IF(!IsRecovery(), "Object referenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
}

void TObjectManager::UnrefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto handler = GetHandler(id);
    i32 refCounter = handler->UnrefObject(id);
    LOG_DEBUG_IF(!IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
    if (refCounter == 0) {
        LOG_DEBUG_IF(!IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
            ~handler->GetType().ToString(),
            ~id.ToString());
    }
}

i32 TObjectManager::GetObjectRefCounter(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return GetHandler(id)->GetObjectRefCounter(id);
}

void TObjectManager::SaveKeys(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.SaveKeys(output);
}

void TObjectManager::SaveValues(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Save(output, TypeToCounter);
    Attributes.SaveValues(output);
}

void TObjectManager::LoadKeys(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.LoadKeys(input);
}

void TObjectManager::LoadValues(TInputStream* input, TLoadContext context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Load(input, TypeToCounter);

    Attributes.LoadValues(input, context);
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    for (int i = 0; i < MaxObjectType; ++i) {
        TypeToCounter[i].Reset();
    }
}

bool TObjectManager::ObjectExists(const TObjectId& id)
{
    return GetHandler(id)->Exists(id);
}

IObjectProxy::TPtr TObjectManager::FindProxy(const TVersionedObjectId& id)
{
    // (NullObjectId, NullTransactionId) means the root transaction.
    if (id.ObjectId == NullObjectId && id.TransactionId == NullTransactionId) {
        return Bootstrap->GetTransactionManager()->GetRootTransactionProxy();
    }

    auto type = TypeFromId(id.ObjectId);
    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
    }

    auto handler = TypeToHandler[typeValue];
    if (!handler) {
        return NULL;
    }

    if (!handler->Exists(id.ObjectId)) {
        return NULL;
    }

    return handler->GetProxy(id);
}

IObjectProxy::TPtr TObjectManager::GetProxy(const TVersionedObjectId& id)
{
    auto proxy = FindProxy(id);
    YASSERT(proxy);
    return proxy;
}

TAttributeSet* TObjectManager::CreateAttributes(const TVersionedObjectId& id)
{
    auto result = new TAttributeSet();
    Attributes.Insert(id, result);
    return result;
}

void TObjectManager::RemoveAttributes(const TVersionedObjectId& id)
{
    Attributes.Remove(id);
}

void TObjectManager::BranchAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    UNUSED(originatingId);
    UNUSED(branchedId);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    auto* originatingAttributes = FindAttributes(originatingId);
    const auto* branchedAttributes = FindAttributes(branchedId);
    if (!branchedAttributes) {
        return;
    }
    if (!originatingAttributes) {
        Attributes.Insert(
            originatingId,
            Attributes.Release(branchedId));
    } else {
        FOREACH (const auto& pair, branchedAttributes->Attributes()) {
            if (pair.second.empty() && !originatingId.IsBranched()) {
                originatingAttributes->Attributes().erase(pair.first);
            } else {
                originatingAttributes->Attributes()[pair.first] = pair.second;
            }
        }
    }
    Attributes.Remove(branchedId);
}

void TObjectManager::ExecuteVerb(
    const TVersionedObjectId& id,
    bool isWrite,
    IServiceContext* context,
    IParamAction<NRpc::IServiceContext*>::TPtr action)
{
    LOG_INFO_IF(!IsRecovery(), "Executing a %s request (Path: %s, Verb: %s, ObjectId: %s, TransactionId: %s)",
        isWrite ? "read-write" : "read-only",
        ~context->GetPath(),
        ~context->GetVerb(),
        ~id.ObjectId.ToString(),
        ~id.TransactionId.ToString());

    if (MetaStateManager->GetStateStatus() != EPeerStatus::Leading ||
        !isWrite ||
        MetaStateManager->IsInCommit())
    {
        action->Do(context);
        return;
    }

    TMsgExecuteVerb message;
    message.set_object_id(id.ObjectId.ToProto());
    message.set_transaction_id(id.TransactionId.ToProto());

    auto requestMessage = context->GetRequestMessage();
    FOREACH (const auto& part, requestMessage->GetParts()) {
        message.add_request_parts(part.Begin(), part.Size());
    }

    auto context_ = MakeStrong(context);
    auto action_ = MakeStrong(action);

    auto wrappedContext = New<TServiceContextWrapper>(context);

    auto change = CreateMetaChange(
        ~MetaStateManager,
        message,
        FromFunctor([=] () -> TVoid
            {
                action_->Do(~wrappedContext);
                return TVoid();
            }));

    change
        ->OnSuccess(FromFunctor([=] (TVoid)
            {
                wrappedContext->Flush();
            }))
        ->OnError(FromFunctor([=] ()
            {
                context_->Reply(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Error committing meta state changes"));
            }))
        ->Commit();
}

TVoid TObjectManager::ReplayVerb(const TMsgExecuteVerb& message)
{
    TVersionedObjectId id(
        TObjectId::FromProto(message.object_id()),
        TTransactionId::FromProto(message.transaction_id()));

    yvector<TSharedRef> parts(message.request_parts_size());
    for (int partIndex = 0; partIndex < static_cast<int>(message.request_parts_size()); ++partIndex) {
        // Construct a non-owning TSharedRef to avoid copying.
        // This is feasible since the message will outlive the request.
        const auto& part = message.request_parts(partIndex);
        parts[partIndex] = TSharedRef::FromRefNonOwning(TRef(const_cast<char*>(part.begin()), part.size()));
    }

    auto requestMessage = CreateMessageFromParts(MoveRV(parts));
    auto header = GetRequestHeader(~requestMessage);
    TYPath path = header.path();
    Stroka verb = header.verb();

    auto context = CreateYPathContext(
        ~requestMessage,
        path,
        verb,
        "",
        NULL);

    auto proxy = GetProxy(id);

    proxy->Invoke(~context);

    return TVoid();
}

DEFINE_METAMAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, Attributes)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

