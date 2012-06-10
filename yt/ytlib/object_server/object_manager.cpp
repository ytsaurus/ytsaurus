#include "stdafx.h"
#include "object_manager.h"
#include "config.h"
#include "object_service_proxy.h"

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_format.h>
#include <ytlib/ytree/serialize.h>

#include <ytlib/cell_master/load_context.h>

#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/transaction.h>

#include <ytlib/rpc/message.h>

#include <ytlib/cypress/cypress_manager.h>

#include <ytlib/cell_master/bootstrap.h>

#include <ytlib/profiling/profiler.h>

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
static NProfiling::TProfiler Profiler("/object_server");

////////////////////////////////////////////////////////////////////////////////

//! A wrapper that is used to postpone a reply until the change is committed by quorum.
class TObjectManager::TServiceContextWrapper
    : public IServiceContext
{
public:
    explicit TServiceContextWrapper(IServiceContextPtr underlyingContext)
        : UnderlyingContext(underlyingContext)
        , Replied(false)
    { }

    virtual NBus::IMessagePtr GetRequestMessage() const
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

    virtual yvector<TSharedRef>& RequestAttachments()
    {
        return UnderlyingContext->RequestAttachments();
    }

    virtual std::vector<TSharedRef>& ResponseAttachments()
    {
        return UnderlyingContext->ResponseAttachments();
    }

    virtual IAttributeDictionary& RequestAttributes()
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

    virtual TClosure Wrap(TClosure action) 
    {
        return UnderlyingContext->Wrap(action);
    }

private:
    IServiceContextPtr UnderlyingContext;
    TError Error;
    bool Replied;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        if (path.empty()) {
            ythrow yexception() << "YPath cannot be empty";
        }

        TTransaction* transaction = NULL;
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();
        auto transactionManager = Bootstrap->GetTransactionManager();
        TObjectId objectId;

        TTokenizer tokenizer(path);
        tokenizer.ParseNext();

        if (tokenizer.GetCurrentType() == TransactionMarkerToken) {
            tokenizer.ParseNext();
            Stroka transactionToken(tokenizer.CurrentToken().GetStringValue());
            tokenizer.ParseNext();
            TTransactionId transactionId;
            if (!TObjectId::FromString(transactionToken, &transactionId)) {
                ythrow yexception() << Sprintf("Error parsing transaction id %s", ~Stroka(transactionToken).Quote());
            }
            if (transactionId != NullTransactionId) {
                transaction = transactionManager->FindTransaction(transactionId);
                if (!transaction) {
                    ythrow yexception() <<  Sprintf("No such transaction %s", ~transactionId.ToString());
                }
            }
        }

        if (tokenizer.GetCurrentType() == RootToken) {
            objectId = cypressManager->GetRootNodeId();
        } else if (tokenizer.GetCurrentType() == NodeGuidMarkerToken) {
            tokenizer.ParseNext();
            Stroka objectToken(tokenizer.CurrentToken().GetStringValue());
            if (!TObjectId::FromString(objectToken, &objectId)) {
                ythrow yexception() << Sprintf("Error parsing object id %s", ~Stroka(objectToken).Quote());
            }
        } else {
            ythrow yexception() << "Invalid YPath syntax";
        }

        auto proxy = objectManager->FindProxy(objectId, transaction);
        if (!proxy) {
            ythrow yexception() << Sprintf("No such object %s", ~objectId.ToString());
        }

        return TResolveResult::There(proxy, TYPath(tokenizer.GetCurrentSuffix()));
    }

    virtual void Invoke(IServiceContextPtr context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const
    {
        return NObjectServer::Logger.GetCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const
    {
        UNUSED(context);
        YUNREACHABLE();
    }

private:
    TBootstrap* Bootstrap;
};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    TObjectManagerConfigPtr config,
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

    auto transactionManager = bootstrap->GetTransactionManager();
    transactionManager->SubscribeTransactionCommitted(BIND(
        &TThis::OnTransactionCommitted,
        MakeStrong(this)));
    transactionManager->SubscribeTransactionAborted(BIND(
        &TThis::OnTransactionAborted,
        MakeStrong(this)));

    auto metaState = bootstrap->GetMetaState();
    TLoadContext context(bootstrap);
    metaState->RegisterLoader(
        "ObjectManager.Keys.1",
        BIND(&TObjectManager::LoadKeys, MakeStrong(this)));
    metaState->RegisterLoader(
        "ObjectManager.Values.1",
        BIND(&TObjectManager::LoadValues, MakeStrong(this), context));
    metaState->RegisterSaver(
        "ObjectManager.Keys.1",
        BIND(&TObjectManager::SaveKeys, MakeStrong(this)),
        ESavePhase::Keys);
    metaState->RegisterSaver(
        "ObjectManager.Values.1",
        BIND(&TObjectManager::SaveValues, MakeStrong(this)),
        ESavePhase::Values);

    metaState->RegisterPart(this);

    RegisterMethod(BIND(&TObjectManager::ReplayVerb, Unretained(this)));

    LOG_INFO("Object Manager initialized (CellId: %d)",
        static_cast<int>(config->CellId));
}

IYPathServicePtr TObjectManager::GetRootService()
{
    return RootService;
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

    LOG_DEBUG_UNLESS(IsRecovery(), "Object id generated (Type: %s, Id: %s)",
        ~type.ToString(),
        ~id.ToString());

    return id;
}

void TObjectManager::RefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = GetHandler(id)->RefObject(id);
    LOG_DEBUG_UNLESS(IsRecovery(), "Object referenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
}

void TObjectManager::RefObject(const TVersionedNodeId& id)
{
    RefObject(id.ObjectId);
}

void TObjectManager::RefObject(TObjectWithIdBase* object)
{
    RefObject(object->GetId());
}

void TObjectManager::RefObject(ICypressNode* node)
{
    RefObject(node->GetId().ObjectId);
}

void TObjectManager::UnrefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto handler = GetHandler(id);
    i32 refCounter = handler->UnrefObject(id);
    LOG_DEBUG_UNLESS(IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
    if (refCounter == 0) {
        LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
            ~handler->GetType().ToString(),
            ~id.ToString());
    }
}

void TObjectManager::UnrefObject(const TVersionedNodeId& id)
{
    UnrefObject(id.ObjectId);
}

void TObjectManager::UnrefObject(TObjectWithIdBase* object)
{
    UnrefObject(object->GetId());
}

void TObjectManager::UnrefObject(ICypressNode* node)
{
    UnrefObject(node->GetId().ObjectId);
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

void TObjectManager::LoadValues(TLoadContext context, TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Load(input, TypeToCounter);

    Attributes.LoadValues(context, input);
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
    auto handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return false;
    }
    return handler->Exists(id);
}

IObjectProxyPtr TObjectManager::FindProxy(
    const TObjectId& id,
    TTransaction* transaction)
{
    // (NullObjectId, NullTransaction) means the root transaction->
    if (id == NullObjectId && !transaction) {
        return Bootstrap->GetTransactionManager()->GetRootTransactionProxy();
    }

    auto type = TypeFromId(id);

    auto handler = FindHandler(type);
    if (!handler) {
        return NULL;
    }

    if (!handler->Exists(id)) {
        return NULL;
    }

    return handler->GetProxy(id, transaction);
}

IObjectProxyPtr TObjectManager::GetProxy(
    const TObjectId& id,
    TTransaction* transaction)
{
    auto proxy = FindProxy(id, transaction);
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
    IServiceContextPtr context,
    TCallback<void(NRpc::IServiceContextPtr)> action)
{
    LOG_INFO_UNLESS(IsRecovery(), "Executing %s request with path %s (ObjectId: %s, TransactionId: %s, IsWrite: %s)",
        ~context->GetVerb(),
        ~context->GetPath().Quote(),
        ~id.ObjectId.ToString(),
        ~id.TransactionId.ToString(),
        ~FormatBool(isWrite));

    auto profilingPath = "/types/" +
        TypeFromId(id.ObjectId).ToString() +
        "/verbs/" +
        context->GetVerb() +
        "/time";

    if (MetaStateManager->GetStateStatus() != EPeerStatus::Leading ||
        !isWrite ||
        MetaStateManager->IsInCommit())
    {
        PROFILE_TIMING (profilingPath) {
            action.Run(context);
        }
        return;
    }

    TMsgExecuteVerb message;
    *message.mutable_object_id() = id.ObjectId.ToProto();
    *message.mutable_transaction_id() = id.TransactionId.ToProto();

    auto requestMessage = context->GetRequestMessage();
    FOREACH (const auto& part, requestMessage->GetParts()) {
        message.add_request_parts(part.Begin(), part.Size());
    }

    auto wrappedContext = New<TServiceContextWrapper>(context);

    auto change = CreateMetaChange(
        MetaStateManager,
        message,
        BIND([=] () -> TVoid {
            PROFILE_TIMING (profilingPath) {
                action.Run(~wrappedContext);
            }
            return TVoid();
        }));

    change
        ->OnSuccess(BIND([=] (TVoid) {
            wrappedContext->Flush();
        }))
        ->OnError(BIND([=] () {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Error committing meta state changes"));
        }))
        ->Commit();
}

TVoid TObjectManager::ReplayVerb(const TMsgExecuteVerb& message)
{
    auto objectId = TObjectId::FromProto(message.object_id());
    auto transactionId = TTransactionId::FromProto(message.transaction_id());

    auto transactionManager = Bootstrap->GetTransactionManager();
    auto* transaction =
        transactionId == NullTransactionId
        ?  NULL
        : &transactionManager->GetTransaction(transactionId);

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
        NYTree::TYPathResponseHandler());

    auto proxy = GetProxy(objectId, transaction);

    proxy->Invoke(~context);

    return TVoid();
}

void TObjectManager::OnTransactionCommitted(TTransaction* transaction)
{
    if (transaction->GetParent()) {
        PromoteCreatedObjects(transaction);
    } else {
        ReleaseCreatedObjects(transaction);
    }
}

void TObjectManager::OnTransactionAborted(TTransaction* transaction)
{
    ReleaseCreatedObjects(transaction);
}

void TObjectManager::PromoteCreatedObjects(TTransaction* transaction)
{
    auto parentTransaction = transaction->GetParent();
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& objectId, transaction->CreatedObjectIds()) {
        YCHECK(parentTransaction->CreatedObjectIds().insert(objectId).second);
    }
}

void TObjectManager::ReleaseCreatedObjects(TTransaction* transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& objectId, transaction->CreatedObjectIds()) {
        objectManager->UnrefObject(objectId);
    }
}

DEFINE_METAMAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, Attributes)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

