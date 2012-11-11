#include "stdafx.h"
#include "object_manager.h"
#include "config.h"
#include "private.h"
#include "gc.h"

#include <ytlib/misc/delayed_invoker.h>

#include <ytlib/ypath/tokenizer.h>

#include <ytlib/rpc/message.h>
#include <ytlib/rpc/server_detail.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <server/cell_master/serialization_context.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NObjectServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NYPath;
using namespace NMetaState;
using namespace NRpc;
using namespace NBus;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NMetaState;
using namespace NObjectServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ObjectServerLogger;
static NProfiling::TProfiler& Profiler = ObjectServerProfiler;

////////////////////////////////////////////////////////////////////////////////

//! A wrapper that is used to postpone the reply until the mutation is committed by quorum.
class TObjectManager::TServiceContextWrapper
    : public NRpc::TServiceContextWrapper
{
public:
    explicit TServiceContextWrapper(IServiceContextPtr underlyingContext)
        : NRpc::TServiceContextWrapper(MoveRV(underlyingContext))
        , Replied(false)
    { }

    virtual bool IsReplied() const override
    {
        return Replied;
    }

    virtual void Reply(const TError& error) override
    {
        YCHECK(!Replied);
        Replied = true;
        Error = error;
    }

    virtual void Reply(IMessagePtr responseMessage) override
    {
        UNUSED(responseMessage);
        YUNREACHABLE();
    }

    virtual const TError& GetError() const override
    {
        return Error;
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

    virtual TResolveResult Resolve(
        const TYPath& path,
        NRpc::IServiceContextPtr context) override
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();
        auto transactionManager = Bootstrap->GetTransactionManager();

        TTransaction* transaction = NULL;
        auto transactionId = GetTransactionId(context);
        if (transactionId != NullTransactionId) {
            transaction = transactionManager->FindTransaction(transactionId);
            if (!transaction) {
                THROW_ERROR_EXCEPTION("No such transaction: %s", ~ToString(transactionId));
            }
            if (transaction->GetState() != ETransactionState::Active) {
                THROW_ERROR_EXCEPTION("Transaction is not active: %s", ~ToString(transactionId));
            }
        }

        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
                THROW_ERROR_EXCEPTION("YPath cannot be empty");

            case NYPath::ETokenType::Slash: {
                auto root = cypressManager->FindVersionedNodeProxy(
                    cypressManager->GetRootNodeId(),
                    transaction);
                return TResolveResult::There(root, tokenizer.GetSuffix());
            }

            case NYPath::ETokenType::Literal: {
                const auto& token = tokenizer.GetToken();
                if (!token.has_prefix(ObjectIdPathPrefix)) {
                    tokenizer.ThrowUnexpected();
                }

                TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
                TObjectId objectId;
                if (!TObjectId::FromString(objectIdString, &objectId)) {
                    THROW_ERROR_EXCEPTION("Error parsing object id: %s", ~objectIdString);
                }

                auto proxy = objectManager->FindProxy(objectId, transaction);
                if (!proxy) {
                    THROW_ERROR_EXCEPTION("No such object: %s", ~ToString(objectId));
                }

                return TResolveResult::There(proxy, tokenizer.GetSuffix());
            }

            default:
                tokenizer.ThrowUnexpected();
                YUNREACHABLE();
        }
    }

    virtual void Invoke(IServiceContextPtr context) override
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return NObjectServer::Logger.GetCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const override
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
    , RootService(New<TRootService>(Bootstrap))
    , GarbageCollector(New<TGarbageCollector>(Config, Bootstrap))
    , CreatedObjectCounter("/destroyed_object_count")
    , DestroyedObjectCounter("/destroyed_object_count")
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
    
    {
        NCellMaster::TLoadContext context;
        context.SetBootstrap(Bootstrap);

        RegisterLoader(
            "ObjectManager.Keys",
            SnapshotVersionValidator(),
            BIND(&TObjectManager::LoadKeys, MakeStrong(this)),
            context);
        RegisterLoader(
            "ObjectManager.Values",
            SnapshotVersionValidator(),
            BIND(&TObjectManager::LoadValues, MakeStrong(this)),
            context);
    }
    {
        NCellMaster::TSaveContext context;

        RegisterSaver(
            ESavePriority::Keys,
            "ObjectManager.Keys",
            CurrentSnapshotVersion,
            BIND(&TObjectManager::SaveKeys, MakeStrong(this)),
            context);
        RegisterSaver(
            ESavePriority::Values,
            "ObjectManager.Values",
            CurrentSnapshotVersion,
            BIND(&TObjectManager::SaveValues, MakeStrong(this)),
            context);
    }

    RegisterMethod(BIND(&TObjectManager::ReplayVerb, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::DestroyObjects, Unretained(this)));

    LOG_INFO("Object Manager initialized (CellId: %d)",
        static_cast<int>(config->CellId));
}

void TObjectManager::Start()
{
    GarbageCollector->Start();
}

IYPathServicePtr TObjectManager::GetRootService()
{
    return RootService;
}

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YCHECK(handler);
    int typeValue = handler->GetType().ToValue();
    YCHECK(typeValue >= 0 && typeValue < MaxObjectType);
    YCHECK(!TypeToHandler[typeValue]);
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

const TGuid& TObjectManager::GetCellGuid() const
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

    auto random = mutationContext->RandomGenerator().Generate<ui64>();

    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);

    auto cellId = GetCellId();

    TObjectId id(
        random,
        (cellId << 16) + typeValue,
        version.RecordCount,
        version.SegmentId);

    Profiler.Increment(CreatedObjectCounter, +1);

    LOG_DEBUG_UNLESS(IsRecovery(), "Object created (Type: %s, Id: %s)",
        ~type.ToString(),
        ~id.ToString());

    return id;
}

void TObjectManager::RefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int refCounter = GetHandler(id)->RefObject(id);
    OnObjectReferenced(id, refCounter);
}

void TObjectManager::RefObject(const TVersionedNodeId& id)
{
    RefObject(id.ObjectId);
}

void TObjectManager::RefObject(TObjectWithIdBase* object)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int refCounter = object->RefObject();
    OnObjectReferenced(object->GetId(), refCounter);
}

void TObjectManager::RefObject(ICypressNode* node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int refCounter = node->GetTrunkNode()->RefObject();
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

    int refCounter = GetHandler(id)->UnrefObject(id);
    OnObjectUnreferenced(id, refCounter);
}

void TObjectManager::UnrefObject(const TVersionedNodeId& id)
{
    UnrefObject(id.ObjectId);
}

void TObjectManager::UnrefObject(TObjectWithIdBase* object)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int refCounter = object->UnrefObject();
    const auto& id = object->GetId();
    OnObjectUnreferenced(id, refCounter);
}

void TObjectManager::UnrefObject(ICypressNode* node)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int refCounter = node->GetTrunkNode()->UnrefObject();
    const auto& id = node->GetId();
    OnObjectUnreferenced(id.ObjectId, refCounter);
}

void TObjectManager::UnrefObject(TChunkTreeRef ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk:
            UnrefObject(static_cast<TObjectWithIdBase*>(ref.AsChunk()));
            break;
        case EObjectType::ChunkList:
            UnrefObject(static_cast<TObjectWithIdBase*>(ref.AsChunkList()));
            break;
        default:
            YUNREACHABLE();
    }
}

int TObjectManager::GetObjectRefCounter(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return GetHandler(id)->GetObjectRefCounter(id);
}

void TObjectManager::OnObjectReferenced(const TObjectId& id, int refCounter)
{
    LOG_DEBUG_UNLESS(IsRecovery(), "Object referenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
}

void TObjectManager::OnObjectUnreferenced(const TObjectId& id, int refCounter)
{
    LOG_DEBUG_UNLESS(IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);

    if (refCounter == 0) {
        GarbageCollector->Enqueue(id);
    }
}

void TObjectManager::SaveKeys(const NCellMaster::TSaveContext& context) const
{
    Attributes.SaveKeys(context);
}

void TObjectManager::SaveValues(const NCellMaster::TSaveContext& context) const
{
    Attributes.SaveValues(context);
    GarbageCollector->Save(context);
}

void TObjectManager::LoadKeys(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.LoadKeys(context);
}

void TObjectManager::LoadValues(const NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.LoadValues(context);
    // COMPAT(babenko)
    if (context.GetVersion() >= 2) {
        GarbageCollector->Load(context);
    }
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.Clear();
    GarbageCollector->Clear();
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
    VERIFY_THREAD_AFFINITY(StateThread);
    
    auto handler = FindHandler(TypeFromId(id));
    return handler ? handler->Exists(id) : false;
}

IObjectProxyPtr TObjectManager::FindProxy(
    const TObjectId& id,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);

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
    VERIFY_THREAD_AFFINITY(StateThread);

    auto proxy = FindProxy(id, transaction);
    YCHECK(proxy);
    return proxy;
}

TAttributeSet* TObjectManager::CreateAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto result = new TAttributeSet();
    Attributes.Insert(id, result);
    return result;
}

void TObjectManager::RemoveAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Attributes.Remove(id);
}

void TObjectManager::BranchAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    UNUSED(originatingId);
    UNUSED(branchedId);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    VERIFY_THREAD_AFFINITY(StateThread);

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
        Attributes.Remove(branchedId);
    }
}

void TObjectManager::ExecuteVerb(
    const TVersionedObjectId& id,
    bool isWrite,
    IServiceContextPtr context,
    TCallback<void(IServiceContextPtr)> action)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    LOG_INFO_UNLESS(IsRecovery(), "ExecuteVerb: %s %s (ObjectId: %s, TransactionId: %s, IsWrite: %s)",
        ~context->GetVerb(),
        ~context->GetPath(),
        ~id.ObjectId.ToString(),
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
        const auto& requestParts = requestMessage->GetParts();
        FOREACH (const auto& part, requestParts) {
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

TMutationPtr TObjectManager::CreateDestroyObjectsMutation(const NProto::TMetaReqDestroyObjects& request)
{
    return Bootstrap
        ->GetMetaStateFacade()
        ->CreateMutation(this, request, &TThis::DestroyObjects);
}

TFuture<void> TObjectManager::GCCollect()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return GarbageCollector->Collect();
}

void TObjectManager::ReplayVerb(const TMetaReqExecute& request)
{
    VERIFY_THREAD_AFFINITY(StateThread);

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
    auto context = CreateYPathContext(
        requestMessage,
        "",
        TYPathResponseHandler());
    auto proxy = GetProxy(objectId, transaction);
    proxy->Invoke(context);
}

void TObjectManager::DestroyObjects(const NProto::TMetaReqDestroyObjects& request)
{
    FOREACH (const auto& protoId, request.object_ids()) {
        auto id = TObjectId::FromProto(protoId);
        // NB: The order of these two calls matters.
        // Dequeue will check the queue for emptiness and will raise CollectPromise when
        // the latter becomes empty. To enable cascaded GC sweep we don't want this to happen
        // if some ids are added during DestroyObject.
        DestroyObject(id);
        GarbageCollector->Dequeue(id);
    }
}

void TObjectManager::DestroyObject(const TObjectId& id)
{
    auto handler = GetHandler(id);
    handler->Destroy(id);
    
    Profiler.Increment(DestroyedObjectCounter, +1);
    
    LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
        ~handler->GetType().ToString(),
        ~id.ToString());
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

