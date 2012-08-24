#include "stdafx.h"
#include "object_manager.h"
#include "config.h"
#include "object_service_proxy.h"

#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_format.h>

#include <ytlib/cell_master/load_context.h>

#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/transaction.h>

#include <ytlib/rpc/message.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/cypress_server/cypress_manager.h>

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/meta_state_facade.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NObjectServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NProto;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ObjectServer");
static NProfiling::TProfiler Profiler("/object_server");

////////////////////////////////////////////////////////////////////////////////

//! A wrapper that is used to postpone the reply until the mutation is committed by quorum.
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

    virtual void Reply(IMessagePtr responseMessage)
    {
        UNUSED(responseMessage);
        YUNREACHABLE();
    }

    virtual const TError& GetError() const
    {
        return Error;
    }

    virtual TSharedRef GetRequestBody() const
    {
        return UnderlyingContext->GetRequestBody();
    }

    virtual TSharedRef GetResponseBody()
    {
        return UnderlyingContext->GetResponseBody();
    }

    virtual void SetResponseBody(const TSharedRef& responseBody)
    {
        UnderlyingContext->SetResponseBody(responseBody);
    }

    virtual std::vector<TSharedRef>& RequestAttachments()
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


    IMessagePtr GetResponseMessage()
    {
        YASSERT(Replied);
        if (!ResponseMessage) {
            ResponseMessage = CreateResponseMessage(this);
        }
        return ResponseMessage;
    }

private:
    IServiceContextPtr UnderlyingContext;
    bool Replied;
    TError Error;
    IMessagePtr ResponseMessage;

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
                if (transaction->GetState() != NTransactionServer::ETransactionState::Active) {
                    ythrow yexception() <<  Sprintf("Transaction %s is not active", ~transactionId.ToString());
                }
            }
        }

        if (tokenizer.GetCurrentType() == RootToken) {
            auto root = cypressManager->FindVersionedNodeProxy(
                cypressManager->GetRootNodeId(),
                transaction);
            return TResolveResult::There(root, TYPath(tokenizer.GetCurrentSuffix()));
        } else if (tokenizer.GetCurrentType() == NodeGuidMarkerToken) {
            tokenizer.ParseNext();
            Stroka objectIdToken(tokenizer.CurrentToken().GetStringValue());
            TObjectId objectId;
            if (!TObjectId::FromString(objectIdToken, &objectId)) {
                ythrow yexception() << Sprintf("Error parsing object id %s", ~Stroka(objectIdToken).Quote());
            }

            auto proxy = objectManager->FindProxy(objectId, transaction);
            if (!proxy) {
                ythrow yexception() << Sprintf("No such object %s", ~objectId.ToString());
            }

            return TResolveResult::There(proxy, TYPath(tokenizer.GetCurrentSuffix()));
        } else {
            ythrow yexception() << "Invalid YPath syntax";
        }
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
        bootstrap->GetMetaStateFacade()->GetManager(),
        bootstrap->GetMetaStateFacade()->GetState())
    , Config(config)
    , Bootstrap(bootstrap)
    , TypeToHandler(MaxObjectType)
    , RootService(New<TRootService>(bootstrap))
{
    YCHECK(config);
    YCHECK(bootstrap);

    auto transactionManager = bootstrap->GetTransactionManager();
    transactionManager->SubscribeTransactionCommitted(BIND(
        &TThis::OnTransactionCommitted,
        MakeStrong(this)));
    transactionManager->SubscribeTransactionAborted(BIND(
        &TThis::OnTransactionAborted,
        MakeStrong(this)));

    auto metaState = bootstrap->GetMetaStateFacade()->GetState();
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

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YASSERT(handler);
    int typeValue = handler->GetType().ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    YASSERT(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;
}

IObjectTypeHandlerPtr TObjectManager::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
    }

    return TypeToHandler[typeValue];
}

IObjectTypeHandlerPtr TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = FindHandler(type);
    YASSERT(handler);
    return handler;
}

IObjectTypeHandlerPtr TObjectManager::GetHandler(const TObjectId& id) const
{
    return GetHandler(TypeFromId(id));
}

TCellId TObjectManager::GetCellId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config->CellId;
}

TGuid TObjectManager::GetCellGuid() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (CachedCellGuild.IsEmpty()) {
        auto rootService = const_cast<TObjectManager*>(this)->GetRootService();
        CachedCellGuild = ConvertTo<TGuid>(SyncYPathGet(rootService, "//sys/@cell_guid"));
    }

    return CachedCellGuild;
}

TObjectId TObjectManager::GenerateId(EObjectType type)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();

    const auto& version = mutationContext->GetVersion();

    auto random = mutationContext->RandomGenerator().GetNext<ui64>();

    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);

    auto cellId = GetCellId();

    TObjectId id(
        random,
        (cellId << 16) + typeValue,
        version.RecordCount,
        version.SegmentId);

    LOG_DEBUG_UNLESS(IsRecovery(), "Object id generated (Type: %s, Id: %s)",
        ~type.ToString(),
        ~id.ToString());

    return id;
}

void TObjectManager::RefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = GetHandler(id)->RefObject(id);
    OnObjectReferenced(id, refCounter);
}

void TObjectManager::RefObject(const TVersionedNodeId& id)
{
    RefObject(id.ObjectId);
}

void TObjectManager::RefObject(TObjectWithIdBase* object)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = object->RefObject();
    OnObjectReferenced(object->GetId(), refCounter);
}

void TObjectManager::RefObject(ICypressNode* node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = node->GetTrunkNode()->RefObject();
    OnObjectReferenced(node->GetId().ObjectId, refCounter);
}

void TObjectManager::RefObject(TChunkTreeRef ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk:
            RefObject((TObjectWithIdBase*) ref.AsChunk());
            break;
        case EObjectType::ChunkList:
            RefObject((TObjectWithIdBase*) ref.AsChunkList());
            break;
        default:
            YUNREACHABLE();
    }
}

void TObjectManager::UnrefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = GetHandler(id)->UnrefObject(id);
    OnObjectUnreferenced(id, refCounter);
}

void TObjectManager::UnrefObject(const TVersionedNodeId& id)
{
    UnrefObject(id.ObjectId);
}

void TObjectManager::UnrefObject(TObjectWithIdBase* object)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = object->UnrefObject();
    OnObjectUnreferenced(object->GetId(), refCounter);
}

void TObjectManager::UnrefObject(ICypressNode* node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = node->GetTrunkNode()->UnrefObject();
    OnObjectUnreferenced(node->GetId().ObjectId, refCounter);
}

void TObjectManager::UnrefObject(TChunkTreeRef ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk:
            UnrefObject((TObjectWithIdBase*) ref.AsChunk());
            break;
        case EObjectType::ChunkList:
            UnrefObject((TObjectWithIdBase*) ref.AsChunkList());
            break;
        default:
            YUNREACHABLE();
    }
}

i32 TObjectManager::GetObjectRefCounter(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return GetHandler(id)->GetObjectRefCounter(id);
}

void TObjectManager::OnObjectReferenced(const TObjectId& id, i32 refCounter)
{
    LOG_DEBUG_UNLESS(IsRecovery(), "Object referenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
}

void TObjectManager::OnObjectUnreferenced(const TObjectId& id, i32 refCounter)
{
    LOG_DEBUG_UNLESS(IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
    if (refCounter == 0) {
        auto handler = GetHandler(id);
        handler->Destroy(id);
        LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
            ~handler->GetType().ToString(),
            ~id.ToString());
    }
}

void TObjectManager::SaveKeys(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.SaveKeys(output);
}

void TObjectManager::SaveValues(TOutputStream* output)
{
    VERIFY_THREAD_AFFINITY(StateThread);

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

    Attributes.LoadValues(context, input);
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.Clear();
}

void TObjectManager::OnStartRecovery()
{
    Profiler.SetEnabled(false);
}

void TObjectManager::OnStopRecovery()
{
    Profiler.SetEnabled(true);
}

bool TObjectManager::ObjectExists(const TObjectId& id)
{
    auto handler = FindHandler(TypeFromId(id));
    return handler ? handler->Exists(id) : false;
}

IObjectProxyPtr TObjectManager::FindProxy(
    const TObjectId& id,
    TTransaction* transaction)
{
    // (NullObjectId, NullTransaction) means the root transaction.
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
            if (!pair.second && !originatingId.IsBranched()) {
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
    LOG_INFO_UNLESS(IsRecovery(), "ExecuteVerb: %s (ObjectId: %s, Path: %s, TransactionId: %s, IsWrite: %s)",
        ~context->GetVerb(),
        ~id.ObjectId.ToString(),
        ~context->GetPath(),
        ~id.TransactionId.ToString(),
        ~FormatBool(isWrite));

    auto profilingPath = "/types/" +
        TypeFromId(id.ObjectId).ToString() +
        "/verbs/" +
        context->GetVerb() +
        "/time";

    if (IsRecovery() || !isWrite || MetaStateManager->GetMutationContext()) {
        PROFILE_TIMING (profilingPath) {
            action.Run(context);
        }
    } else {
        TMetaReqExecute executeReq;
        *executeReq.mutable_object_id() = id.ObjectId.ToProto();
        *executeReq.mutable_transaction_id() = id.TransactionId.ToProto();

        auto requestMessage = context->GetRequestMessage();
        FOREACH (const auto& part, requestMessage->GetParts()) {
            executeReq.add_request_parts(part.Begin(), part.Size());
        }

        // Capture everything needed in lambdas below.
        auto wrappedContext = New<TServiceContextWrapper>(context);
        auto mutationId = GetRpcMutationId(context);
        auto metaStateManager = MetaStateManager;

        Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation()
            ->SetRequestData(executeReq)
            ->SetId(mutationId)
            ->SetAction(BIND([=] () {
                PROFILE_TIMING (profilingPath) {
                    action.Run(wrappedContext);
                }
                if (mutationId != NullMutationId) {
                    auto responseMessage = wrappedContext->GetResponseMessage();
                    auto responseData = PackMessage(responseMessage);
                    metaStateManager->GetMutationContext()->SetResponseData(responseData);
                }
            }))
            ->OnSuccess(BIND([=] (const TMutationResponse& response) {
                auto responseMessage =
                    response.Applied
                    ? wrappedContext->GetResponseMessage()
                    : UnpackMessage(response.Data);
                context->Reply(responseMessage);
            }))
            ->OnError(CreateRpcErrorHandler(context))
            ->Commit();
    }
}

void TObjectManager::ReplayVerb(const TMetaReqExecute& request)
{
    auto objectId = TObjectId::FromProto(request.object_id());
    auto transactionId = TTransactionId::FromProto(request.transaction_id());

    auto transactionManager = Bootstrap->GetTransactionManager();
    auto* transaction =
        transactionId == NullTransactionId
        ?  NULL
        : transactionManager->GetTransaction(transactionId);

    std::vector<TSharedRef> parts(request.request_parts_size());
    for (int partIndex = 0; partIndex < request.request_parts_size(); ++partIndex) {
        // Construct a non-owning TSharedRef to avoid copying.
        // This is feasible since the message will outlive the request.
        const auto& part = request.request_parts(partIndex);
        parts[partIndex] = TSharedRef::FromRefNonOwning(TRef(const_cast<char*>(part.begin()), part.size()));
    }

    auto requestMessage = CreateMessageFromParts(MoveRV(parts));
    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));

    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    auto context = CreateYPathContext(
        requestMessage,
        path,
        verb,
        "",
        TYPathResponseHandler());

    auto proxy = GetProxy(objectId, transaction);

    proxy->Invoke(context);
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
    transaction->CreatedObjectIds().clear();
}

void TObjectManager::ReleaseCreatedObjects(TTransaction* transaction)
{
    auto objectManager = Bootstrap->GetObjectManager();
    FOREACH (const auto& objectId, transaction->CreatedObjectIds()) {
        objectManager->UnrefObject(objectId);
    }
    transaction->CreatedObjectIds().clear();
}

DEFINE_METAMAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, Attributes)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

