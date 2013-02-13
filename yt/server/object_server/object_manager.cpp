#include "stdafx.h"
#include "object_manager.h"
#include "object.h"
#include "config.h"
#include "private.h"
#include "gc.h"
#include "attribute_set.h"

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
        : NRpc::TServiceContextWrapper(std::move(underlyingContext))
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
        YCHECK(Replied);
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

        TTransaction* transaction = nullptr;
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
                auto root = cypressManager->GetVersionedNodeProxy(
                    cypressManager->GetRootNode(),
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

                // TODO(babenko): special handling for null transaction
                if (objectId == NullObjectId && !transaction) {
                    auto proxy = Bootstrap->GetTransactionManager()->GetRootTransactionProxy();
                    return TResolveResult::There(proxy, tokenizer.GetSuffix());
                }

                auto* object = objectManager->FindObject(objectId);
                if (!object || !object->IsAlive()) {
                    THROW_ERROR_EXCEPTION("No such object: %s", ~ToString(objectId));
                }

                auto proxy = objectManager->GetProxy(object, transaction);
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

    // TODO(panin): remove this when getting rid of IAttributeProvider
    virtual void SerializeAttributes(
        NYson::IYsonConsumer* consumer,
        const TAttributeFilter& filter) const override
    {
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
        return nullptr;
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

IObjectTypeHandlerPtr TObjectManager::GetHandler(TObjectBase* obj) const
{
    return GetHandler(TypeFromId(obj->GetId()));
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

void TObjectManager::RefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    LOG_DEBUG_UNLESS(IsRecovery(), "Object referenced (Id: %s, RefCounter: %d)",
        ~object->GetId().ToString(),
        refCounter);
}

void TObjectManager::UnrefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject();
    LOG_DEBUG_UNLESS(IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d)",
        ~object->GetId().ToString(),
        refCounter);

    if (refCounter == 0) {
        GarbageCollector->Enqueue(object->GetId());
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
    GarbageCollector->Load(context);
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

void TObjectManager::OnActiveQuorumEstablished()
{
    GarbageCollector->StartSweep();
}

void TObjectManager::OnStopLeading()
{
    GarbageCollector->StopSweep();
}

TObjectBase* TObjectManager::FindObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->FindObject(id);
}

TObjectBase* TObjectManager::GetObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* object = FindObject(id);
    YCHECK(object);
    return object;
}

IObjectProxyPtr TObjectManager::GetProxy(
    TObjectBase* object,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(StateThread);
    YCHECK(object);
    YCHECK(object->IsAlive());

    const auto& id = object->GetId();
    auto handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->GetProxy(object, transaction);
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
        auto attributeSet = Attributes.Release(branchedId);
        Attributes.Insert(originatingId, attributeSet.Release());
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
        if (!Bootstrap->GetMetaStateFacade()->IsActiveLeader()) {
            context->Reply(TError(
                EErrorCode::Unavailable,
                "Not an active leader"));
            return;
        }

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
        ? nullptr
        : transactionManager->GetTransaction(transactionId);

    std::vector<TSharedRef> parts(request.request_parts_size());
    for (int partIndex = 0; partIndex < request.request_parts_size(); ++partIndex) {
        // Construct a non-owning TSharedRef to avoid copying.
        // This is feasible since the message will outlive the request.
        const auto& part = request.request_parts(partIndex);
        parts[partIndex] = TSharedRef::FromRefNonOwning(TRef(const_cast<char*>(part.begin()), part.size()));
    }

    auto requestMessage = CreateMessageFromParts(std::move(parts));
    auto context = CreateYPathContext(
        requestMessage,
        "",
        TYPathResponseHandler());

    // TODO(babenko): special handling for null transaction
    if (objectId == NullObjectId && !transaction) {
        auto proxy = Bootstrap->GetTransactionManager()->GetRootTransactionProxy();
        proxy->Invoke(context);
        return;
    }

    auto* object = GetObject(objectId);
    auto proxy = GetProxy(object, transaction);
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
    auto handler = GetHandler(TypeFromId(id));
    handler->Destroy(id);

    Profiler.Increment(DestroyedObjectCounter, +1);

    LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
        ~handler->GetType().ToString(),
        ~id.ToString());
}

DEFINE_METAMAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, Attributes)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

