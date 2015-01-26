#include "stdafx.h"
#include "object_manager.h"
#include "object.h"
#include "config.h"
#include "private.h"
#include "garbage_collector.h"
#include "attribute_set.h"
#include "schema.h"
#include "master.h"

#include <core/ypath/tokenizer.h>

#include <core/rpc/response_keeper.h>

#include <core/erasure/public.h>

#include <core/profiling/profile_manager.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <server/cell_master/serialize.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/chunk_server/chunk_list.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

#include <server/security_server/user.h>
#include <server/security_server/group.h>
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

static const auto& Logger = ObjectServerLogger;
static const auto ProfilingPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TResolveResult Resolve(const TYPath& path, IServiceContextPtr context) override
    {
        auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

        if (ypathExt.mutating()) {
            // Mutating request.

            if (hydraManager->IsMutating()) {
                // Nested call.
                return DoResolveThere(path, std::move(context));
            }

            if (hydraManager->IsRecovery()) {
                // Recovery.
                return DoResolveThere(path, std::move(context));
            }

            // Commit mutation.
            return DoResolveHere(path);
        } else {
            // Read-only request.
            return DoResolveThere(path, context);
        }
    }

    virtual void Invoke(IServiceContextPtr context) override
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        auto userId = user->GetId();

        auto responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
        auto mutationId = GetMutationId(context);
        if (mutationId != NullMutationId) {
            auto asyncResponseMessage = responseKeeper->TryBeginRequest(mutationId);
            if (asyncResponseMessage) {
                context->ReplyFrom(std::move(asyncResponseMessage));
                return;
            }
        }

        NProto::TReqExecute request;
        ToProto(request.mutable_user_id(), userId);
        // TODO(babenko): optimize, use multipart records
        auto requestMessage = context->GetRequestMessage();
        for (const auto& part : requestMessage) {
            request.add_request_parts(part.Begin(), part.Size());
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager
            ->CreateExecuteMutation(request)
            ->SetAction(
                BIND(
                    &TObjectManager::HydraExecuteLeader,
                    objectManager,
                    userId,
                    mutationId,
                    context))
            ->Commit()
            .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                if (!result.IsOK()) {
                    // Reply with commit error.
                    context->Reply(result);
                }
            }));
    }

    virtual NLog::TLogger GetLogger() const override
    {
        return ObjectServerLogger;
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
    TBootstrap* const Bootstrap_;


    static TResolveResult DoResolveHere(const TYPath& path)
    {
        return TResolveResult::Here(path);
    }

    TResolveResult DoResolveThere(const TYPath& path, IServiceContextPtr context)
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto objectManager = Bootstrap_->GetObjectManager();
        auto transactionManager = Bootstrap_->GetTransactionManager();

        TTransaction* transaction = nullptr;
        auto transactionId = GetTransactionId(context);
        if (transactionId != NullTransactionId) {
            transaction = transactionManager->GetTransactionOrThrow(transactionId);
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
                    THROW_ERROR_EXCEPTION("Error parsing object id %v",
                        objectIdString);
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
        : Bootstrap_(bootstrap)
    { }

    virtual IObjectProxyPtr ResolvePath(const TYPath& path, TTransaction* transaction) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto cypressManager = Bootstrap_->GetCypressManager();

        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
                return objectManager->GetMasterProxy();

            case NYPath::ETokenType::Slash: {
                auto root = cypressManager->GetNodeProxy(
                    cypressManager->GetRootNode(),
                    transaction);
                return DoResolvePath(root, transaction, tokenizer.GetSuffix());
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
                        "Error parsing object id %v",
                        objectIdString);
                }

                auto* object = objectManager->GetObjectOrThrow(objectId);
                auto proxy = objectManager->GetProxy(object, transaction);
                return DoResolvePath(proxy, transaction, tokenizer.GetSuffix());
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
            auto* nodeProxy = dynamic_cast<ICypressNodeProxy*>(proxy.Get());
            auto resolver = nodeProxy->GetResolver();
            return resolver->GetPath(nodeProxy);
        } else {
            return FromObjectId(id);
        }
    }

private:
    TBootstrap* Bootstrap_;


    IObjectProxyPtr DoResolvePath(
        IObjectProxyPtr proxy,
        TTransaction* transaction,
        const TYPath& path)
    {
        // Fast path.
        if (path.empty()) {
            return std::move(proxy);
        }

        // Slow path.
        auto req = TObjectYPathProxy::GetBasicAttributes(path);
        auto rsp = SyncExecuteVerb(proxy, req);
        auto objectId = FromProto<TObjectId>(rsp->id());

        auto objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->GetObjectOrThrow(objectId);
        return objectManager->GetProxy(object, transaction);
    }

};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    TObjectManagerConfigPtr config,
    TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap)
    , Config_(config)
    , Profiler(ObjectServerProfiler)
    , RootService_(New<TRootService>(Bootstrap_))
    , ObjectResolver_(new TObjectResolver(Bootstrap_))
    , GarbageCollector_(New<TGarbageCollector>(Config_, Bootstrap_))
{
    YCHECK(config);
    YCHECK(bootstrap);

    RegisterLoader(
        "ObjectManager.Keys",
        BIND(&TObjectManager::LoadKeys, Unretained(this)));
    RegisterLoader(
        "ObjectManager.Values",
        BIND(&TObjectManager::LoadValues, Unretained(this)));
    // COMPAT(babenko)
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

    RegisterHandler(CreateMasterTypeHandler(Bootstrap_));

    RegisterMethod(BIND(&TObjectManager::HydraExecuteFollower, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraDestroyObjects, Unretained(this)));

    MasterObjectId_ = MakeWellKnownId(EObjectType::Master, Bootstrap_->GetCellTag());
}

void TObjectManager::Initialize()
{
    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(),
        BIND(&TObjectManager::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor_->Start();
}

IYPathServicePtr TObjectManager::GetRootService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootService_;
}

TObjectBase* TObjectManager::GetMasterObject()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterObject_.get();
}

IObjectProxyPtr TObjectManager::GetMasterProxy()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterProxy_;
}

TObjectBase* TObjectManager::FindSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return TypeToEntry_[type].SchemaObject;
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

    const auto& entry = TypeToEntry_[type];
    YCHECK(entry.SchemaProxy);
    return entry.SchemaProxy;
}

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YCHECK(handler);

    auto type = handler->GetType();
    YCHECK(!TypeToEntry_[type].Handler);
    YCHECK(RegisteredTypes_.insert(type).second);
    auto& entry = TypeToEntry_[type];
    entry.Handler = handler;
    entry.TagId = NProfiling::TProfileManager::Get()->RegisterTag("type", type);
    if (HasSchema(type)) {
        auto schemaType = SchemaTypeFromType(type);
        auto& schemaEntry = TypeToEntry_[schemaType];
        schemaEntry.Handler = CreateSchemaTypeHandler(Bootstrap_, type);
        LOG_INFO("Type registered (Type: %v, SchemaObjectId: %v)",
            type,
            MakeSchemaObjectId(type, Bootstrap_->GetCellTag()));
    } else {
        LOG_INFO("Type registered (Type: %v)",
            type);
    }
}

IObjectTypeHandlerPtr TObjectManager::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return TypeToEntry_[type].Handler;
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

const std::set<EObjectType>& TObjectManager::GetRegisteredTypes() const
{
    return RegisteredTypes_;
}

TObjectId TObjectManager::GenerateId(EObjectType type)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* mutationContext = Bootstrap_
        ->GetHydraFacade()
        ->GetHydraManager()
        ->GetMutationContext();

    const auto& version = mutationContext->GetVersion();

    auto random = mutationContext->RandomGenerator().Generate<ui64>();

    TObjectId id(
        random,
        (Bootstrap_->GetCellTag() << 16) + static_cast<int>(type),
        version.RecordId,
        version.SegmentId);

    ++CreatedObjectCount_;

    LOG_DEBUG_UNLESS(IsRecovery(), "Object created (Type: %v, Id: %v)",
        type,
        id);

    return id;
}

void TObjectManager::RefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    LOG_TRACE_UNLESS(IsRecovery(), "Object referenced (Id: %v, RefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        object->GetObjectWeakRefCounter());
}

void TObjectManager::UnrefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject();
    LOG_TRACE_UNLESS(IsRecovery(), "Object unreferenced (Id: %v, RefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        object->GetObjectWeakRefCounter());

    if (refCounter == 0) {
        GarbageCollector_->Enqueue(object);
    }
}

void TObjectManager::WeakRefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    int weakRefCounter = object->WeakRefObject();
    if (weakRefCounter == 1) {
        ++LockedObjectCount_;
    }
}

void TObjectManager::WeakUnrefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    int weakRefCounter = object->WeakUnrefObject();
    if (weakRefCounter == 0) {
        --LockedObjectCount_;
        if (!object->IsAlive()) {
            GarbageCollector_->Unlock(object);
        }
    }
}

void TObjectManager::SaveKeys(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveKeys(context);
    AttributeMap_.SaveKeys(context);
}

void TObjectManager::SaveValues(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveValues(context);
    AttributeMap_.SaveValues(context);
    GarbageCollector_->Save(context);
}

void TObjectManager::OnBeforeSnapshotLoaded()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoClear();
}

void TObjectManager::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // COMPAT(babenko)
    if (context.GetVersion() >= 109) {
        SchemaMap_.LoadKeys(context);
    }

    AttributeMap_.LoadKeys(context);
}

void TObjectManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // COMPAT(babenko)
    if (context.GetVersion() >= 109) {
        SchemaMap_.LoadValues(context);
        for (const auto& pair : SchemaMap_) {
            auto type = TypeFromSchemaType(TypeFromId(pair.first));
            YCHECK(RegisteredTypes_.find(type) != RegisteredTypes_.end());
            auto& entry = TypeToEntry_[type];
            entry.SchemaObject = pair.second;
            entry.SchemaProxy = CreateSchemaProxy(Bootstrap_, entry.SchemaObject);
        }
    }

    AttributeMap_.LoadValues(context);

    GarbageCollector_->Load(context);
}

// COMPAT(babenko)
void TObjectManager::LoadSchemas(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    while (true) {
        EObjectType type;
        Load(context, type);
        if (type == EObjectType::Null)
            break;

        const auto& entry = TypeToEntry_[type];
        entry.SchemaObject->Load(context);
    }
}

void TObjectManager::DoClear()
{
    MasterObject_.reset(new TMasterObject(MasterObjectId_));
    MasterObject_->RefObject();

    MasterProxy_ = CreateMasterProxy(Bootstrap_, MasterObject_.get());

    AttributeMap_.Clear();

    GarbageCollector_->Clear();

    CreatedObjectCount_ = 0;
    DestroyedObjectCount_ = 0;
    LockedObjectCount_ = 0;

    SchemaMap_.Clear();
    for (auto type : RegisteredTypes_) {
        auto& entry = TypeToEntry_[type];
        if (HasSchema(type)) {
            auto id = MakeSchemaObjectId(type, Bootstrap_->GetCellTag());
            entry.SchemaObject = new TSchemaObject(id);
            SchemaMap_.Insert(id, entry.SchemaObject);
            entry.SchemaObject->RefObject();
            entry.SchemaProxy = CreateSchemaProxy(Bootstrap_, entry.SchemaObject);
        }
    }
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    DoClear();
}

void TObjectManager::OnRecoveryStarted()
{
    Profiler.SetEnabled(false);

    GarbageCollector_->UnlockAll();
    LockedObjectCount_ = 0;
}

void TObjectManager::OnRecoveryComplete()
{
    Profiler.SetEnabled(true);
}

void TObjectManager::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector_->Start();
}

void TObjectManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector_->Stop();
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
            "No such object %v",
            id);
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
    AttributeMap_.Insert(id, result);
    return result;
}

void TObjectManager::RemoveAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    AttributeMap_.Remove(id);
}

bool TObjectManager::TryRemoveAttributes(const TVersionedObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return AttributeMap_.TryRemove(id);
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
        auto attributeSet = AttributeMap_.Release(branchedId);
        AttributeMap_.Insert(originatingId, attributeSet.release());
    } else {
        for (const auto& pair : branchedAttributes->Attributes()) {
            if (!pair.second && !originatingId.IsBranched()) {
                originatingAttributes->Attributes().erase(pair.first);
            } else {
                originatingAttributes->Attributes()[pair.first] = pair.second;
            }
        }
        AttributeMap_.Remove(branchedId);
    }
}

void TObjectManager::FillAttributes(
    TObjectBase* object,
    const IAttributeDictionary& attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(!IsVersionedType(object->GetType()));

    auto keys = attributes.List();
    if (keys.empty())
        return;

    auto* attributeSet = GetOrCreateAttributes(TVersionedObjectId(object->GetId()));
    for (const auto& key : keys) {
        YCHECK(attributeSet->Attributes().insert(std::make_pair(
            key,
            attributes.GetYson(key))).second);
    }
}

TMutationPtr TObjectManager::CreateExecuteMutation(const NProto::TReqExecute& request)
{
    return CreateMutation(
        Bootstrap_->GetHydraFacade()->GetHydraManager(),
        request,
        this,
        &TObjectManager::HydraExecuteFollower);
}

TMutationPtr TObjectManager::CreateDestroyObjectsMutation(const NProto::TReqDestroyObjects& request)
{
    return CreateMutation(
        Bootstrap_->GetHydraFacade()->GetHydraManager(),
        request,
        this,
        &TObjectManager::HydraDestroyObjects);
}

TFuture<void> TObjectManager::GCCollect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GarbageCollector_->Collect();
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
        THROW_ERROR_EXCEPTION("Unknown object type %v",
            type);
    }

    auto options = handler->GetCreationOptions();
    if (!options) {
        THROW_ERROR_EXCEPTION("Instances of type %Qlv cannot be created directly",
            type);
    }

    switch (options->TransactionMode) {
        case EObjectTransactionMode::Required:
            if (!transaction) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %Qlv outside of a transaction",
                    type);
            }
            break;

        case EObjectTransactionMode::Forbidden:
            if (transaction) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %Qlv inside of a transaction",
                    type);
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
                THROW_ERROR_EXCEPTION("Cannot create an instance of %Qlv without an account",
                    type);
            }
            break;

        case EObjectAccountMode::Forbidden:
            if (account) {
                THROW_ERROR_EXCEPTION("Cannot create an instance of %Qlv with an account",
                    type);
            }
            break;

        case EObjectAccountMode::Optional:
            break;

        default:
            YUNREACHABLE();
    }

    auto securityManager = Bootstrap_->GetSecurityManager();
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

    FillAttributes(object, *attributes);

    auto* stagingTransaction = handler->GetStagingTransaction(object);
    if (stagingTransaction) {
        YCHECK(transaction == stagingTransaction);
        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->StageObject(transaction, object);
    } else {
        YCHECK(object->GetObjectRefCounter() > 0);
    }

    auto* acd = securityManager->FindAcd(object);
    if (acd) {
        acd->SetOwner(user);
    }

    return object;
}

IObjectResolver* TObjectManager::GetObjectResolver()
{
    return ObjectResolver_.get();
}

bool TObjectManager::AdviceYield(TInstant startTime) const
{
    return TInstant::Now() > startTime + Config_->YieldTimeout;
}

void TObjectManager::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
{
    auto transactionManager = Bootstrap_->GetTransactionManager();
    auto cypressManager = Bootstrap_->GetCypressManager();

    auto getPrerequisiteTransaction = [&] (const TTransactionId& transactionId) {
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is missing",
                transactionId);
        }
        if (transaction->GetState() != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is not active",
                transactionId);
        }
        return transaction;
    };

    for (const auto& prerequisite : prerequisites.transactions()) {
        auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
        getPrerequisiteTransaction(transactionId);
    }

    for (const auto& prerequisite : prerequisites.revisions()) {
        auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
        const auto& path = prerequisite.path();
        i64 revision = prerequisite.revision();

        auto* transaction = transactionId == NullTransactionId
            ? nullptr
            : getPrerequisiteTransaction(transactionId);

        auto resolver = cypressManager->CreateResolver(transaction);
        INodePtr nodeProxy;
        try {
            nodeProxy = resolver->ResolvePath(path);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: failed to resolve path %v",
                path)
                << ex;
        }

        auto* cypressNodeProxy = dynamic_cast<ICypressNodeProxy*>(nodeProxy.Get());
        YCHECK(cypressNodeProxy);

        auto* node = cypressNodeProxy->GetTrunkNode();
        if (node->GetRevision() != revision) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: node %v revision mismatch: expected %v, found %v",
                path,
                revision,
                node->GetRevision());
        }
    }
}

void TObjectManager::HydraExecuteLeader(
    const TUserId& userId,
    const TMutationId& mutationId,
    IServiceContextPtr context)
{
    auto asyncResponseMessage = context->GetAsyncResponseMessage();

    try {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetUserOrThrow(userId);
        TAuthenticatedUserGuard userGuard(securityManager, user);
        ExecuteVerb(RootService_, context);
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }

    if (mutationId != NullMutationId) {
        auto responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
        asyncResponseMessage.Subscribe(
            BIND([=] (const TErrorOr<TSharedRefArray>& messageOrError) {
                if (messageOrError.IsOK()) {
                    responseKeeper->EndRequest(mutationId, messageOrError.Value());
                }
            }));
    }
}

void TObjectManager::HydraExecuteFollower(const NProto::TReqExecute& request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto userId = FromProto<TUserId>(request.user_id());

    std::vector<TSharedRef> parts(request.request_parts_size());
    for (int partIndex = 0; partIndex < request.request_parts_size(); ++partIndex) {
        parts[partIndex] = TSharedRef::FromString(request.request_parts(partIndex));
    }

    auto requestMessage = TSharedRefArray(std::move(parts));

    auto context = CreateYPathContext(std::move(requestMessage));

    auto mutationId = GetMutationId(context);

    HydraExecuteLeader(
        userId,
        mutationId,
        std::move(context));
}

void TObjectManager::HydraDestroyObjects(const NProto::TReqDestroyObjects& request)
{
    for (const auto& protoId : request.object_ids()) {
        auto id = FromProto<TObjectId>(protoId);
        auto type = TypeFromId(id);
        auto handler = GetHandler(type);
        auto* object = handler->GetObject(id);

        // NB: The order of Dequeue/Destroy/CheckEmpty calls matters.
        // CheckEmpty will raise CollectPromise_ when GC queue becomes empty.
        // To enable cascaded GC sweep we don't want this to happen
        // if some ids are added during DestroyObject.
        GarbageCollector_->Dequeue(object);
        handler->Destroy(object);
        ++DestroyedObjectCount_;

        LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %v, Id: %v)",
            type,
            id);
    }

    GarbageCollector_->CheckEmpty();
}


NProfiling::TProfiler& TObjectManager::GetProfiler()
{
    return Profiler;
}

NProfiling::TTagId TObjectManager::GetTypeTagId(EObjectType type)
{
    return TypeToEntry_[type].TagId;
}

NProfiling::TTagId TObjectManager::GetMethodTagId(const Stroka& method)
{
    auto it = MethodToTag_.find(method);
    if (it != MethodToTag_.end()) {
        return it->second;
    }
    auto tag = NProfiling::TProfileManager::Get()->RegisterTag("method", method);
    YCHECK(MethodToTag_.insert(std::make_pair(method, tag)).second);
    return tag;
}

void TObjectManager::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Profiler.Enqueue("/gc_queue_size", GarbageCollector_->GetGCQueueSize());
    Profiler.Enqueue("/gc_lock_queue_size", GarbageCollector_->GetLockedGCQueueSize());
    Profiler.Enqueue("/created_object_count", CreatedObjectCount_);
    Profiler.Enqueue("/destroyed_object_count", DestroyedObjectCount_);
    Profiler.Enqueue("/locked_object_count", LockedObjectCount_);
}

DEFINE_ENTITY_MAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, AttributeMap_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

