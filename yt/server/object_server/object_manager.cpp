#include "stdafx.h"
#include "object_manager.h"
#include "object.h"
#include "config.h"
#include "private.h"
#include "gc.h"
#include "attribute_set.h"
#include "schema.h"
#include "master.h"

#include <core/concurrency/delayed_executor.h>

#include <core/ypath/tokenizer.h>

#include <core/rpc/message.h>
#include <core/rpc/server_detail.h>

#include <core/erasure/public.h>

#include <core/profiling/profiling_manager.h>

#include <ytlib/hydra/rpc_helpers.h>

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

#include <server/security_server/user.h>
#include <server/security_server/group.h>
#include <server/security_server/acl.h>
#include <server/security_server/security_manager.h>

namespace NYT {
namespace NObjectServer {

using namespace NYTree;
using namespace NYPath;
using namespace NHydra;
using namespace NRpc;
using namespace NBus;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NHydra;
using namespace NCellMaster;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ObjectServerLogger;
static TDuration ProfilingPeriod = TDuration::MilliSeconds(100);

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

    virtual void Reply(TSharedRefArray responseMessage) override
    {
        UNUSED(responseMessage);
        YUNREACHABLE();
    }

    virtual const TError& GetError() const override
    {
        return Error;
    }

    TSharedRefArray GetResponseMessage()
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
    TSharedRefArray ResponseMessage;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    {
        Logger = ObjectServerLogger;
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
        const auto& headerExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (headerExt.mutating() &&
            !hydraManager->IsMutating() &&
            !hydraManager->IsRecovery())
        {
            return TResolveResult::Here(path);
        } else {
            return DoResolve(path, std::move(context));
        }
    }

    virtual void Invoke(IServiceContextPtr context) override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        auto userId = user->GetId();

        auto mutationId = GetMutationId(context);

        NProto::TReqExecute request;
        ToProto(request.mutable_user_id(), userId);
        // TODO(babenko): optimize, use multipart records
        auto requestMessage = context->GetRequestMessage();
        for (const auto& part : requestMessage) {
            request.add_request_parts(part.Begin(), part.Size());
        }

        auto objectManager = Bootstrap->GetObjectManager();
        objectManager
            ->CreateExecuteMutation(request)
            ->SetId(mutationId)
            ->SetAction(BIND(&TObjectManager::ExecuteMutatingRequest, objectManager, userId, context))
            ->OnError(CreateRpcErrorHandler(context))
            ->Commit();
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return ObjectServerLogger.GetCategory();
    }

    // TODO(panin): remove this when getting rid of IAttributeProvider
    virtual void SerializeAttributes(
        NYson::IYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*filter*/,
        bool /*sortKeys*/) override
    {
        YUNREACHABLE();
    }

private:
    TBootstrap* Bootstrap;

    TResolveResult DoResolve(
        const TYPath& path,
        IServiceContextPtr context)
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();
        auto transactionManager = Bootstrap->GetTransactionManager();

        TTransaction* transaction = nullptr;
        auto transactionId = GetTransactionId(context);
        if (transactionId != NullTransactionId) {
            transaction = transactionManager->GetTransactionOrThrow(transactionId);
            if (transaction->GetState() != ETransactionState::Active) {
                THROW_ERROR_EXCEPTION(
                    "Transaction %s is not active",
                    ~ToString(transactionId));
            }
        }

        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
                return TResolveResult::There(objectManager->GetMasterProxy(), tokenizer.GetSuffix());

            case NYPath::ETokenType::Slash: {
                auto root = cypressManager->GetNodeProxy(
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
                    THROW_ERROR_EXCEPTION("Error parsing object id %s",
                        ~objectIdString);
                }

                auto* object = objectManager->GetObjectOrThrow(objectId);
                auto proxy = objectManager->GetProxy(object, transaction);
                return TResolveResult::There(proxy, tokenizer.GetSuffix());
            }

            default:
                tokenizer.ThrowUnexpected();
                YUNREACHABLE();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TObjectResolver
    : public IObjectResolver
{
public:
    explicit TObjectResolver(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual IObjectProxyPtr ResolvePath(const TYPath& path, TTransaction* transaction) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto cypressManager = Bootstrap->GetCypressManager();

        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
                return objectManager->GetMasterProxy();

            case NYPath::ETokenType::Slash: {
                auto root = cypressManager->GetNodeProxy(
                    cypressManager->GetRootNode(),
                    transaction);
                return DoResolvePath(root, tokenizer.GetSuffix());
            }

            case NYPath::ETokenType::Literal: {
                const auto& token = tokenizer.GetToken();
                if (!token.has_prefix(ObjectIdPathPrefix)) {
                    tokenizer.ThrowUnexpected();
                }

                TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
                TObjectId objectId;
                if (!TObjectId::FromString(objectIdString, &objectId)) {
                    THROW_ERROR_EXCEPTION(
                        NYTree::EErrorCode::ResolveError,
                        "Error parsing object id %s",
                        ~objectIdString);
                }

                auto* object = objectManager->GetObjectOrThrow(objectId);
                auto proxy = objectManager->GetProxy(object, transaction);
                return DoResolvePath(proxy, tokenizer.GetSuffix());
            }

            default:
                tokenizer.ThrowUnexpected();
                YUNREACHABLE();
        }
    }

    virtual TYPath GetPath(IObjectProxyPtr proxy) override
    {
        const auto& id = proxy->GetId();
        if (IsVersionedType(TypeFromId(id))) {
            auto* nodeProxy = dynamic_cast<ICypressNodeProxy*>(~proxy);
            auto resolver = nodeProxy->GetResolver();
            return resolver->GetPath(nodeProxy);
        } else {
            return FromObjectId(id);
        }
    }

private:
    TBootstrap* Bootstrap;

    static IObjectProxyPtr DoResolvePath(IObjectProxyPtr proxy, const TYPath& path)
    {
        if (path.empty()) {
            return std::move(proxy);
        }

        auto* nodeProxy = dynamic_cast<ICypressNodeProxy*>(~proxy);
        if (!nodeProxy) {
            THROW_ERROR_EXCEPTION(
                "Cannot resolve nontrivial path %s for nonversioned object %s",
                NYTree::EErrorCode::ResolveError,
                ~path,
                ~ToString(proxy->GetId()));
        }

        auto resolvedNode = GetNodeByYPath(nodeProxy, path);
        auto* resolvedNodeProxy = dynamic_cast<ICypressNodeProxy*>(~resolvedNode);
        YCHECK(resolvedNodeProxy);
        return resolvedNodeProxy;
    }

};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    TObjectManagerConfigPtr config,
    TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap)
    , Config(config)
    , Profiler(ObjectServerProfiler)
    , RootService(New<TRootService>(bootstrap))
    , ObjectResolver(new TObjectResolver(bootstrap))
    , GarbageCollector(New<TGarbageCollector>(config, bootstrap))
    , CreatedObjectCount(0)
    , DestroyedObjectCount(0)
    , LockedObjectCount(0)
{
    YCHECK(config);
    YCHECK(bootstrap);

    RegisterLoader(
        "ObjectManager.Keys",
        BIND(&TObjectManager::LoadKeys, Unretained(this)));
    RegisterLoader(
        "ObjectManager.Values",
        BIND(&TObjectManager::LoadValues, Unretained(this)));
    RegisterLoader(
        "ObjectManager.Schemas",
        BIND(&TObjectManager::LoadSchemas, Unretained(this)));

    RegisterSaver(
        ESerializationPriority::Keys,
        "ObjectManager.Keys",
        BIND(&TObjectManager::SaveKeys, Unretained(this)));
    RegisterSaver(
        ESerializationPriority::Values,
        "ObjectManager.Values",
        BIND(&TObjectManager::SaveValues, Unretained(this)));
    RegisterSaver(
        ESerializationPriority::Values,
        "ObjectManager.Schemas",
        BIND(&TObjectManager::SaveSchemas, Unretained(this)));

    RegisterHandler(CreateMasterTypeHandler(Bootstrap));

    RegisterMethod(BIND(&TObjectManager::HydraExecute, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraDestroyObjects, Unretained(this)));

    MasterObjectId = MakeWellKnownId(EObjectType::Master, Bootstrap->GetCellId());
}

void TObjectManager::Initialize()
{
    ProfilingExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetMetaStateFacade()->GetInvoker(),
        BIND(&TObjectManager::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor->Start();
}

IYPathServicePtr TObjectManager::GetRootService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootService;
}

TObjectBase* TObjectManager::GetMasterObject()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ~MasterObject;
}

IObjectProxyPtr TObjectManager::GetMasterProxy()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterProxy;
}

TObjectBase* TObjectManager::FindSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = static_cast<int>(type);
    if (typeValue < 0 || typeValue > MaxObjectType) {
        return nullptr;
    }

    return ~TypeToEntry[typeValue].SchemaObject;
}

TObjectBase* TObjectManager::GetSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* schema = FindSchema(type);
    YCHECK(schema);
    return schema;
}

IObjectProxyPtr TObjectManager::GetSchemaProxy(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = static_cast<int>(type);
    YCHECK(typeValue >= 0 && typeValue <= MaxObjectType);

    const auto& entry = TypeToEntry[typeValue];
    YCHECK(entry.SchemaProxy);
    return entry.SchemaProxy;
}

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YCHECK(handler);

    auto type = handler->GetType();
    int typeValue = static_cast<int>(type);
    YCHECK(typeValue >= 0 && typeValue <= MaxObjectType);
    YCHECK(!TypeToEntry[typeValue].Handler);

    RegisteredTypes.push_back(type);
    auto& entry = TypeToEntry[typeValue];
    entry.Handler = handler;
    entry.TagId = NProfiling::TProfilingManager::Get()->RegisterTag("type", type);
    if (HasSchema(type)) {
        auto schemaType = SchemaTypeFromType(type);
        auto& schemaEntry = TypeToEntry[static_cast<int>(schemaType)];
        schemaEntry.Handler = CreateSchemaTypeHandler(Bootstrap, type);
        LOG_INFO("Type registered (Type: %s, SchemaObjectId: %s)",
            ~type.ToString(),
            ~ToString(MakeSchemaObjectId(type, Bootstrap->GetCellId())));
    } else {
        LOG_INFO("Type registered (Type: %s)",
            ~type.ToString());
    }
}

IObjectTypeHandlerPtr TObjectManager::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = static_cast<int>(type);
    if (typeValue < 0 || typeValue > MaxObjectType) {
        return nullptr;
    }

    return TypeToEntry[typeValue].Handler;
}

IObjectTypeHandlerPtr TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = FindHandler(type);
    YASSERT(handler);
    return handler;
}

IObjectTypeHandlerPtr TObjectManager::GetHandler(TObjectBase* object) const
{
    return GetHandler(object->GetType());
}

const std::vector<EObjectType> TObjectManager::GetRegisteredTypes() const
{
    return RegisteredTypes;
}

TObjectId TObjectManager::GenerateId(EObjectType type)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* mutationContext = Bootstrap
        ->GetMetaStateFacade()
        ->GetManager()
        ->GetMutationContext();

    const auto& version = mutationContext->GetVersion();

    auto random = mutationContext->RandomGenerator().Generate<ui64>();

    int typeValue = static_cast<int>(type);
    YASSERT(typeValue >= 0 && typeValue <= MaxObjectType);

    TObjectId id(
        random,
        (Bootstrap->GetCellId() << 16) + typeValue,
        version.RecordId,
        version.SegmentId);

    ++CreatedObjectCount;

    LOG_DEBUG_UNLESS(IsRecovery(), "Object created (Type: %s, Id: %s)",
        ~type.ToString(),
        ~ToString(id));

    return id;
}

void TObjectManager::RefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    LOG_TRACE_UNLESS(IsRecovery(), "Object referenced (Id: %s, RefCounter: %d, WeakRefCounter: %d)",
        ~ToString(object->GetId()),
        refCounter,
        object->GetObjectWeakRefCounter());
}

void TObjectManager::UnrefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject();
    LOG_TRACE_UNLESS(IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d, WeakRefCounter: %d)",
        ~ToString(object->GetId()),
        refCounter,
        object->GetObjectWeakRefCounter());

    if (refCounter == 0) {
        GarbageCollector->Enqueue(object);
    }
}

void TObjectManager::WeakRefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    int weakRefCounter = object->WeakRefObject();
    if (weakRefCounter == 1) {
        ++LockedObjectCount;
    }
}

void TObjectManager::WeakUnrefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    int weakRefCounter = object->WeakUnrefObject();
    if (weakRefCounter == 0) {
        --LockedObjectCount;
        if (!object->IsAlive()) {
            GarbageCollector->Unlock(object);
        }
    }
}

void TObjectManager::SaveKeys(NCellMaster::TSaveContext& context) const
{
    Attributes.SaveKeys(context);
}

void TObjectManager::SaveValues(NCellMaster::TSaveContext& context) const
{
    Attributes.SaveValues(context);
    GarbageCollector->Save(context);
}

void TObjectManager::SaveSchemas(NCellMaster::TSaveContext& context) const
{
    // Make sure the ordering of RegisteredTypes does not matter.
    auto types = RegisteredTypes;
    std::sort(types.begin(), types.end());

    for (auto type : types) {
        if (HasSchema(type)) {
            Save(context, type);
            const auto& entry = TypeToEntry[static_cast<int>(type)];
            entry.SchemaObject->Save(context);
        }
    }

    // Write a sentinel.
    Save(context, EObjectType(EObjectType::Null));
}

void TObjectManager::OnBeforeSnapshotLoaded()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoClear();
}

void TObjectManager::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Attributes.LoadKeys(context);
}

void TObjectManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Attributes.LoadValues(context);
    GarbageCollector->Load(context);

       // COMPAT(psushin)
    if (context.GetVersion() < 21) {
        for (const auto& pair : Attributes) {
            auto type = TypeFromId(pair.first.ObjectId);
            if ((type == EObjectType::Table || type == EObjectType::File) && pair.first.TransactionId == NullTransactionId) {
                auto& attributes = pair.second->Attributes();
                if (attributes.find("erasure_codec") == attributes.end()) {
                    auto value = NYTree::ConvertToYsonString(NErasure::ECodec(NErasure::ECodec::None));
                    YCHECK(attributes.insert(std::make_pair("erasure_codec", MakeNullable(value))).second);
                }
            }
        }
    }
}

void TObjectManager::LoadSchemas(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    while (true) {
        EObjectType type;
        Load(context, type);
        if (type == EObjectType::Null)
            break;

        const auto& entry = TypeToEntry[static_cast<int>(type)];
        entry.SchemaObject->Load(context);
    }
}

void TObjectManager::DoClear()
{
    MasterObject.reset(new TMasterObject(MasterObjectId));
    MasterObject->RefObject();

    MasterProxy = CreateMasterProxy(Bootstrap, ~MasterObject);

    for (auto type : RegisteredTypes)  {
        auto& entry = TypeToEntry[static_cast<int>(type)];
        if (HasSchema(type)) {
            entry.SchemaObject.reset(new TSchemaObject(MakeSchemaObjectId(type, Bootstrap->GetCellId())));
            entry.SchemaObject->RefObject();
            entry.SchemaProxy = CreateSchemaProxy(Bootstrap, ~entry.SchemaObject);
        }
    }

    Attributes.Clear();

    GarbageCollector->Clear();

    CreatedObjectCount = 0;
    DestroyedObjectCount = 0;
    LockedObjectCount = 0;
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoClear();
}

void TObjectManager::OnRecoveryStarted()
{
    Profiler.SetEnabled(false);

    GarbageCollector->UnlockAll();
    LockedObjectCount = 0;
}

void TObjectManager::OnRecoveryComplete()
{
    Profiler.SetEnabled(true);
}

void TObjectManager::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector->StartSweep();
}

void TObjectManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector->StopSweep();
}

TObjectBase* TObjectManager::FindObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->FindObject(id);
}

TObjectBase* TObjectManager::GetObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* object = FindObject(id);
    YCHECK(object);
    return object;
}

TObjectBase* TObjectManager::GetObjectOrThrow(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* object = FindObject(id);
    if (!IsObjectAlive(object)) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "No such object %s",
            ~ToString(id));
    }

    return object;
}

IObjectProxyPtr TObjectManager::GetProxy(
    TObjectBase* object,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(IsObjectAlive(object));

    const auto& id = object->GetId();
    auto handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->GetProxy(object, transaction);
}

TAttributeSet* TObjectManager::GetOrCreateAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* userAttributes = FindAttributes(id);
    if (!userAttributes) {
        userAttributes = CreateAttributes(id);
    }

    return userAttributes;
}

TAttributeSet* TObjectManager::CreateAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto result = new TAttributeSet();
    Attributes.Insert(id, result);
    return result;
}

void TObjectManager::RemoveAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Attributes.Remove(id);
}

bool TObjectManager::TryRemoveAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return Attributes.TryRemove(id);
}

void TObjectManager::BranchAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    UNUSED(originatingId);
    UNUSED(branchedId);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* originatingAttributes = FindAttributes(originatingId);
    const auto* branchedAttributes = FindAttributes(branchedId);
    if (!branchedAttributes) {
        return;
    }

    if (!originatingAttributes) {
        auto attributeSet = Attributes.Release(branchedId);
        Attributes.Insert(originatingId, attributeSet.release());
    } else {
        for (const auto& pair : branchedAttributes->Attributes()) {
            if (!pair.second && !originatingId.IsBranched()) {
                originatingAttributes->Attributes().erase(pair.first);
            } else {
                originatingAttributes->Attributes()[pair.first] = pair.second;
            }
        }
        Attributes.Remove(branchedId);
    }
}

TMutationPtr TObjectManager::CreateExecuteMutation(const NProto::TReqExecute& request)
{
    return Bootstrap
        ->GetMetaStateFacade()
        ->CreateMutation(this, request, &TThis::HydraExecute);
}

TMutationPtr TObjectManager::CreateDestroyObjectsMutation(const NProto::TReqDestroyObjects& request)
{
    return Bootstrap
        ->GetMetaStateFacade()
        ->CreateMutation(this, request, &TThis::HydraDestroyObjects);
}

TFuture<void> TObjectManager::GCCollect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GarbageCollector->Collect();
}

TObjectBase* TObjectManager::CreateObject(
    TTransaction* transaction,
    TAccount* account,
    EObjectType type,
    IAttributeDictionary* attributes,
    IObjectTypeHandler::TReqCreateObjects* request,
    IObjectTypeHandler::TRspCreateObjects* response)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto handler = FindHandler(type);
    if (!handler) {
        THROW_ERROR_EXCEPTION("Unknown object type %s",
            ~type.ToString());
    }

    auto options = handler->GetCreationOptions();
    if (!options) {
        THROW_ERROR_EXCEPTION("Instances of type %s cannot be created directly",
            ~FormatEnum(type).Quote());
    }

    switch (options->TransactionMode) {
        case EObjectTransactionMode::Required:
            if (!transaction) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %s outside of a transaction",
                    ~FormatEnum(type).Quote());
            }
            break;

        case EObjectTransactionMode::Forbidden:
            if (transaction) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %s inside of a transaction",
                    ~FormatEnum(type).Quote());
            }
            break;

        case EObjectTransactionMode::Optional:
            break;

        default:
            YUNREACHABLE();
    }

    switch (options->AccountMode) {
        case EObjectAccountMode::Required:
            if (!account) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %s without an account",
                    ~FormatEnum(type).Quote());
            }
            break;

        case EObjectAccountMode::Forbidden:
            if (account) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %s with an account",
                    ~FormatEnum(type).Quote());
            }
            break;

        case EObjectAccountMode::Optional:
            break;

        default:
            YUNREACHABLE();
    }

    auto securityManager = Bootstrap->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto* schema = FindSchema(type);
    if (schema) {
        securityManager->ValidatePermission(schema, user, EPermission::Create);
    }

    auto* object = handler->Create(
        transaction,
        account,
        attributes,
        request,
        response);
    const auto& objectId = object->GetId();

    // Copy attributes. Quick and dirty.
    auto attributeKeys = attributes->List();
    if (!attributeKeys.empty()) {
        auto* attributeSet = GetOrCreateAttributes(TVersionedObjectId(objectId));

        for (const auto& key : attributeKeys) {
            YCHECK(attributeSet->Attributes().insert(std::make_pair(
                key,
                attributes->GetYson(key))).second);
        }
    }

    if (transaction && options->SupportsStaging) {
        auto transactionManager = Bootstrap->GetTransactionManager();
        transactionManager->StageObject(transaction, object);
    }

    auto* acd = securityManager->FindAcd(object);
    if (acd) {
        acd->SetOwner(user);
    }

    return object;
}

IObjectResolver* TObjectManager::GetObjectResolver()
{
    return ~ObjectResolver;
}

void TObjectManager::InterceptProxyInvocation(TObjectProxyBase* proxy, IServiceContextPtr context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // Validate that mutating requests are only being invoked inside mutations or recovery.
    auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
    const auto& headerExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    YCHECK(!headerExt.mutating() ||
           hydraManager->IsMutating() ||
           hydraManager->IsRecovery());

    auto securityManager = Bootstrap->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto objectId = proxy->GetVersionedId();

    LOG_DEBUG_UNLESS(IsRecovery(), "Invoke: %s:%s %s (ObjectId: %s, Mutating: %s, User: %s)",
        ~context->GetService(),
        ~context->GetVerb(),
        ~GetRequestYPath(context),
        ~ToString(objectId),
        ~FormatBool(headerExt.mutating()),
        ~user->GetName());

    NProfiling::TTagIdList tagIds;
    tagIds.push_back(GetTypeTagId(TypeFromId(objectId.ObjectId)));
    tagIds.push_back(GetVerbTagId(context->GetVerb()));

    PROFILE_TIMING ("/request_time", tagIds) {
        proxy->GuardedInvoke(std::move(context));
    }
}

void TObjectManager::ExecuteMutatingRequest(const TUserId& userId, IServiceContextPtr context)
{
    try {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* user = securityManager->GetUserOrThrow(userId);
        TAuthenticatedUserGuard userGuard(securityManager, user);
        ExecuteVerb(RootService, context);
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }

    YCHECK(context->IsReplied());

    auto mutationId = GetMutationId(context);
    if (mutationId != NullMutationId) {
        auto responseMessage = CreateResponseMessage(context);
        auto responseData = responseMessage.Pack();
        auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
        auto* mutationContext = hydraManager->GetMutationContext();
        mutationContext->SetResponseData(std::move(responseData));
    }
}

void TObjectManager::HydraExecute(const NProto::TReqExecute& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto userId = FromProto<TUserId>(request.user_id());

    std::vector<TSharedRef> parts(request.request_parts_size());
    for (int partIndex = 0; partIndex < request.request_parts_size(); ++partIndex) {
        // Construct a non-owning TSharedRef to avoid copying.
        // This is feasible since the message will outlive the request.
        const auto& part = request.request_parts(partIndex);
        parts[partIndex] = TSharedRef::FromRefNonOwning(TRef::FromString(part));
    }

    auto requestMessage = TSharedRefArray(std::move(parts));

    auto context = CreateYPathContext(
        std::move(requestMessage),
        "", // disable logging
        TYPathResponseHandler());

    ExecuteMutatingRequest(userId, std::move(context));
}

void TObjectManager::HydraDestroyObjects(const NProto::TReqDestroyObjects& request)
{
    for (const auto& protoId : request.object_ids()) {
        auto id = FromProto<TObjectId>(protoId);
        auto type = TypeFromId(id);
        auto handler = GetHandler(type);
        auto* object = handler->GetObject(id);

        // NB: The order of Dequeue/Destroy/CheckEmpty calls matters.
        // CheckEmpty will raise CollectPromise when GC becomes empty.
        // To enable cascaded GC sweep we don't want this to happen
        // if some ids are added during DestroyObject.
        GarbageCollector->Dequeue(object);
        handler->Destroy(object);
        ++DestroyedObjectCount;

        LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
            ~type.ToString(),
            ~ToString(id));
    }

    GarbageCollector->CheckEmpty();
}

NProfiling::TTagId TObjectManager::GetTypeTagId(EObjectType type)
{
    return TypeToEntry[type].TagId;
}

NProfiling::TTagId TObjectManager::GetVerbTagId(const Stroka& verb)
{
    auto it = VerbToTag.find(verb);
    if (it != VerbToTag.end()) {
        return it->second;
    }
    auto tag = NProfiling::TProfilingManager::Get()->RegisterTag("verb", verb);
    YCHECK(VerbToTag.insert(std::make_pair(verb, tag)).second);
    return tag;
}

void TObjectManager::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Profiler.Enqueue("/gc_queue_size", GarbageCollector->GetGCQueueSize());
    Profiler.Enqueue("/gc_lock_queue_size", GarbageCollector->GetLockedGCQueueSize());
    Profiler.Enqueue("/created_object_count", CreatedObjectCount);
    Profiler.Enqueue("/destroyed_object_count", DestroyedObjectCount);
    Profiler.Enqueue("/locked_object_count", LockedObjectCount);
}

DEFINE_ENTITY_MAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, Attributes)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

