#include "object_manager.h"
#include "private.h"
#include "config.h"
#include "garbage_collector.h"
#include "master.h"
#include "master_type_handler.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_list.h>

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/node_detail.h>
#include <yt/server/master/cypress_server/path_resolver.h>

#include <yt/server/lib/election/election_manager.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>
#include <yt/server/master/security_server/account.h>

#include <yt/server/master/transaction_server/transaction.h>
#include <yt/server/master/transaction_server/transaction_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/core/erasure/public.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/ytree/node_detail.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/misc/crash_handler.h>

namespace NYT::NObjectServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NHydra;
using namespace NRpc;
using namespace NBus;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NCellMaster;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;
static const auto ProfilingPeriod = TDuration::MilliSeconds(100);
static const IObjectTypeHandlerPtr NullTypeHandler;

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRemoteProxy
    : public IYPathService
{
public:
    TRemoteProxy(TBootstrap* bootstrap, TObjectId objectId)
        : Bootstrap_(bootstrap)
        , ObjectId_(objectId)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& context) override
    {
        return TResolveResultHere{path};
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        auto* mutationContext = TryGetCurrentMutationContext();
        if (mutationContext) {
            // XXX(babenko): portals
            mutationContext->SetResponseKeeperSuppressed(true);
        }

        const auto& hydraManager  = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (ypathExt.mutating() && hydraManager->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        auto requestMessage = context->GetRequestMessage();
        auto requestHeader = context->RequestHeader();

        auto redirectedPath = FromObjectId(ObjectId_) + GetRequestYPath(requestHeader);
        SetRequestYPath(&requestHeader, redirectedPath);
        auto updatedMessage = SetRequestHeader(requestMessage, requestHeader);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto cellTag = CellTagFromId(ObjectId_);
        auto asyncResponseMessage = objectManager->ForwardObjectRequest(
            std::move(updatedMessage),
            cellTag,
            ypathExt.mutating() ? EPeerKind::Leader : EPeerKind::Follower,
            context->GetTimeout());

        asyncResponseMessage
            .Subscribe(
                BIND([
                    bootstrap = Bootstrap_,
                    mutationId = mutationContext ? mutationContext->Request().MutationId : NullMutationId,
                    context
                ] (const TErrorOr<TSharedRefArray>& messageOrError) {
                    if (messageOrError.IsOK()) {
                        context->Reply(messageOrError.Value());
                    } else {
                        context->Reply(TError(messageOrError));
                    }

                    if (mutationId) {
                        const auto& responseKeeper = bootstrap->GetHydraFacade()->GetResponseKeeper();
                        responseKeeper->EndRequest(mutationId, messageOrError, false);
                    }
                }).Via(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService)));
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const std::optional<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    virtual bool ShouldHideAttributes() override
    {
        return false;
    }

private:
    TBootstrap* const Bootstrap_;
    const TObjectId ObjectId_;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& context) override
    {
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (ypathExt.mutating() && !HasMutationContext()) {
            // Nested call or recovery.
            return TResolveResultHere{path};
        } else {
            return DoResolveThere(path, context);
        }
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto mutation = objectManager->CreateExecuteMutation(user->GetName(), context);
        mutation->SetAllowLeaderForwarding(true);
        mutation->Commit()
            .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                if (!result.IsOK()) {
                    // Reply with commit error.
                    context->Reply(result);
                }
            }));
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const std::optional<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    virtual bool ShouldHideAttributes() override
    {
        YT_ABORT();
    }

private:
    TBootstrap* const Bootstrap_;


    TResolveResult DoResolveThere(const TYPath& path, const IServiceContextPtr& context)
    {
        TPathResolver resolver(
            Bootstrap_,
            context->GetService(),
            context->GetMethod(),
            path,
            GetTransactionId(context));
        auto result = resolver.Resolve();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto proxy = Visit(result.Payload,
            [&] (const TPathResolver::TLocalObjectPayload& payload) -> IYPathServicePtr {
                return objectManager->GetProxy(payload.Object, payload.Transaction);
            },
            [&] (const TPathResolver::TRemoteObjectPayload& payload) -> IYPathServicePtr  {
                return objectManager->CreateRemoteProxy(payload.ObjectId);
            },
            [&] (const TPathResolver::TMissingObjectPayload& payload) -> IYPathServicePtr {
                return TNonexistingService::Get();
            });

        return TResolveResultThere{
            std::move(proxy),
            std::move(result.UnresolvedPathSuffix)
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ObjectManager)
    , Profiler(ObjectServerProfiler)
    , RootService_(New<TRootService>(Bootstrap_))
    , GarbageCollector_(New<TGarbageCollector>(Bootstrap_))
{
    YT_VERIFY(bootstrap);

    RegisterLoader(
        "ObjectManager.Keys",
        BIND(&TObjectManager::LoadKeys, Unretained(this)));
    RegisterLoader(
        "ObjectManager.Values",
        BIND(&TObjectManager::LoadValues, Unretained(this)));

    RegisterSaver(
        ESyncSerializationPriority::Keys,
        "ObjectManager.Keys",
        BIND(&TObjectManager::SaveKeys, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "ObjectManager.Values",
        BIND(&TObjectManager::SaveValues, Unretained(this)));

    RegisterHandler(CreateMasterTypeHandler(Bootstrap_));

    RegisterMethod(BIND(&TObjectManager::HydraExecuteFollower, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraDestroyObjects, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraCreateForeignObject, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraRemoveForeignObject, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraUnrefExportedObjects, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraConfirmObjectLifeStage, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraAdvanceObjectLifeStage, Unretained(this)));

    MasterObjectId_ = MakeWellKnownId(EObjectType::Master, Bootstrap_->GetPrimaryCellTag());
}

void TObjectManager::Initialize()
{
    if (Bootstrap_->IsPrimaryMaster()) {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->SubscribeReplicateValuesToSecondaryMaster(
            BIND(&TObjectManager::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
    }

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
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

    auto it = TypeToEntry_.find(type);
    return it == TypeToEntry_.end() ? nullptr : it->second.SchemaObject;
}

TObjectBase* TObjectManager::GetSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* schema = FindSchema(type);
    YT_VERIFY(schema);
    return schema;
}

IObjectProxyPtr TObjectManager::GetSchemaProxy(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto it = TypeToEntry_.find(type);
    YT_VERIFY(it != TypeToEntry_.end());
    const auto& entry = it->second;
    YT_VERIFY(entry.SchemaProxy);
    return entry.SchemaProxy;
}

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YT_VERIFY(handler);

    auto type = handler->GetType();

    TTypeEntry entry;
    entry.Handler = handler;
    YT_VERIFY(TypeToEntry_.emplace(type, entry).second);
    YT_VERIFY(RegisteredTypes_.insert(type).second);

    if (HasSchema(type)) {
        auto schemaType = SchemaTypeFromType(type);
        TypeToEntry_[schemaType].Handler = CreateSchemaTypeHandler(Bootstrap_, type);

        auto schemaObjectId = MakeSchemaObjectId(type, Bootstrap_->GetPrimaryCellTag());

        YT_LOG_INFO("Type registered (Type: %v, SchemaObjectId: %v)",
            type,
            schemaObjectId);
    } else {
        YT_LOG_INFO("Type registered (Type: %v)",
            type);
    }
}

const IObjectTypeHandlerPtr& TObjectManager::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto it = TypeToEntry_.find(type);
    return it == TypeToEntry_.end() ? NullTypeHandler : it->second.Handler;
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& handler = FindHandler(type);
    YT_ASSERT(handler);
    return handler;
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(const TObjectBase* object) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetHandler(object->GetType());
}

const std::set<EObjectType>& TObjectManager::GetRegisteredTypes() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RegisteredTypes_;
}

TObjectId TObjectManager::GenerateId(EObjectType type, TObjectId hintId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* mutationContext = GetCurrentMutationContext();
    auto version = mutationContext->GetVersion();
    auto hash = mutationContext->RandomGenerator().Generate<ui32>();

    auto cellTag = Bootstrap_->GetCellTag();

    auto id = hintId
        ? hintId
        : MakeRegularId(type, cellTag, version, hash);
    YT_ASSERT(TypeFromId(id) == type);

    ++CreatedObjects_;

    return id;
}

int TObjectManager::RefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!object->IsDestroyed());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    YT_LOG_TRACE_UNLESS(IsRecovery(), "Object referenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectEphemeralRefCounter(object),
        GetObjectWeakRefCounter(object));

    if (refCounter == 1) {
        GarbageCollector_->UnregisterZombie(object);
    }

    return refCounter;
}

int TObjectManager::UnrefObject(TObjectBase* object, int count)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->IsAlive());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject(count);
    YT_LOG_TRACE_UNLESS(IsRecovery(), "Object unreferenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectEphemeralRefCounter(object),
        GetObjectWeakRefCounter(object));

    if (refCounter == 0) {
        const auto& handler = GetHandler(object);
        handler->ZombifyObject(object);

        GarbageCollector_->RegisterZombie(object);

        // XXX(babenko): move to ZombifyObject
        if (Bootstrap_->IsPrimaryMaster()) {
            auto flags = handler->GetFlags();
            if (Any(flags & ETypeFlags::ReplicateDestroy)) {
                NProto::TReqRemoveForeignObject request;
                ToProto(request.mutable_object_id(), object->GetId());

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                auto replicationCellTags = handler->GetReplicationCellTags(object);
                multicellManager->PostToMasters(request, replicationCellTags);
            }
        }
    }
    return refCounter;
}

int TObjectManager::GetObjectRefCounter(TObjectBase* object)
{
    return object->GetObjectRefCounter();
}

int TObjectManager::EphemeralRefObject(TObjectBase* object)
{
    return GarbageCollector_->EphemeralRefObject(object, CurrentEpoch_);
}

int TObjectManager::EphemeralUnrefObject(TObjectBase* object)
{
    return GarbageCollector_->EphemeralUnrefObject(object, CurrentEpoch_);
}

int TObjectManager::GetObjectEphemeralRefCounter(TObjectBase* object)
{
    return object->GetObjectEphemeralRefCounter(CurrentEpoch_);
}

int TObjectManager::WeakRefObject(TObjectBase* object)
{
    return GarbageCollector_->WeakRefObject(object, CurrentEpoch_);
}

int TObjectManager::WeakUnrefObject(TObjectBase* object)
{
    return GarbageCollector_->WeakUnrefObject(object, CurrentEpoch_);
}

int TObjectManager::GetObjectWeakRefCounter(TObjectBase* object)
{
    return object->GetObjectWeakRefCounter();
}

void TObjectManager::SaveKeys(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveKeys(context);
    GarbageCollector_->SaveKeys(context);
}

void TObjectManager::SaveValues(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveValues(context);
    GarbageCollector_->SaveValues(context);
}

void TObjectManager::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadKeys(context);
    GarbageCollector_->LoadKeys(context);
}

void TObjectManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadValues(context);

    InitSchemas();

    GarbageCollector_->LoadValues(context);
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::Clear();

    MasterObject_.reset(new TMasterObject(MasterObjectId_));
    MasterObject_->RefObject();

    MasterProxy_ = GetHandler(EObjectType::Master)->GetProxy(MasterObject_.get(), nullptr);

    SchemaMap_.Clear();

    InitSchemas();

    CreatedObjects_ = 0;
    DestroyedObjects_ = 0;

    GarbageCollector_->Clear();
}

void TObjectManager::InitSchemas()
{
    for (auto& pair : TypeToEntry_) {
        auto& entry = pair.second;
        entry.SchemaObject = nullptr;
        entry.SchemaProxy.Reset();
    }

    for (auto type : RegisteredTypes_) {
        if (!HasSchema(type)) {
            continue;
        }

        auto id = MakeSchemaObjectId(type, Bootstrap_->GetPrimaryCellTag());
        if (!SchemaMap_.Contains(id)) {
            auto schemaObject = std::make_unique<TSchemaObject>(id);
            schemaObject->RefObject();
            SchemaMap_.Insert(id, std::move(schemaObject));
        }

        auto& entry = TypeToEntry_[type];
        entry.SchemaObject = SchemaMap_.Get(id);
        entry.SchemaProxy = GetProxy(entry.SchemaObject);
    }
}

void TObjectManager::OnRecoveryStarted()
{
    Profiler.SetEnabled(false);

    ++CurrentEpoch_;
    GarbageCollector_->Reset();
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

TObjectBase* TObjectManager::FindObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->FindObject(id);
}

TObjectBase* TObjectManager::GetObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* object = FindObject(id);
    YT_VERIFY(object);
    return object;
}

TObjectBase* TObjectManager::GetObjectOrThrow(TObjectId id)
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

TObjectBase* TObjectManager::GetWeakGhostObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    return GarbageCollector_->GetWeakGhostObject(id);
}

IYPathServicePtr TObjectManager::CreateRemoteProxy(TObjectId id)
{
    return New<TRemoteProxy>(Bootstrap_, id);
}

IObjectProxyPtr TObjectManager::GetProxy(
    TObjectBase* object,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsObjectAlive(object));

    // Fast path.
    if (object == MasterObject_.get()) {
        return MasterProxy_;
    }

    // Slow path.
    auto id = object->GetId();
    const auto& handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->GetProxy(object, transaction);
}

void TObjectManager::BranchAttributes(
    const TObjectBase* /*originatingObject*/,
    TObjectBase* /*branchedObject*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
    TObjectBase* originatingObject,
    const TObjectBase* branchedObject)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto* branchedAttributes = branchedObject->GetAttributes();
    if (!branchedAttributes)
        return;

    auto* originatingAttributes = originatingObject->GetMutableAttributes();
    for (const auto& pair : branchedAttributes->Attributes()) {
        if (!pair.second && originatingObject->IsTrunk()) {
            originatingAttributes->Attributes().erase(pair.first);
        } else {
            originatingAttributes->Attributes()[pair.first] = pair.second;
        }
    }

    if (originatingAttributes->Attributes().empty()) {
        originatingObject->ClearAttributes();
    }
}

void TObjectManager::FillAttributes(
    TObjectBase* object,
    const IAttributeDictionary& attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto keys = attributes.List();
    if (keys.empty()) {
        return;
    }

    auto proxy = GetProxy(object, nullptr);
    std::sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
        auto value = attributes.GetYson(key);
        proxy->MutableAttributes()->Set(key, value);
    }
}

std::unique_ptr<TMutation> TObjectManager::CreateExecuteMutation(
    const TString& userName,
    const IServiceContextPtr& context)
{
    NProto::TReqExecute request;
    request.set_user_name(userName);
    auto requestMessage = context->GetRequestMessage();
    for (const auto& part : requestMessage) {
        request.add_request_parts(part.Begin(), part.Size());
    }

    auto mutation = CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request);
    mutation->SetHandler(BIND(
        &TObjectManager::HydraExecuteLeader,
        MakeStrong(this),
        userName,
        MakeCodicilData(userName),
        context));
    return mutation;
}

std::unique_ptr<TMutation> TObjectManager::CreateDestroyObjectsMutation(const NProto::TReqDestroyObjects& request)
{
    return CreateMutation(
        Bootstrap_->GetHydraFacade()->GetHydraManager(),
        request,
        &TObjectManager::HydraDestroyObjects,
        this);
}

TFuture<void> TObjectManager::GCCollect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GarbageCollector_->Collect();
}

TObjectBase* TObjectManager::CreateObject(
    TObjectId hintId,
    EObjectType type,
    IAttributeDictionary* attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = FindHandler(type);
    if (!handler) {
        THROW_ERROR_EXCEPTION("Unknown object type %v",
            type);
    }

    auto flags = handler->GetFlags();
    if (None(flags & ETypeFlags::Creatable)) {
        THROW_ERROR_EXCEPTION("Objects of type %Qlv cannot be created explicitly",
            type);
    }

    bool replicate =
        Bootstrap_->IsPrimaryMaster() &&
        Any(flags & ETypeFlags::ReplicateCreate);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto* schema = FindSchema(type);
    if (schema) {
        securityManager->ValidatePermission(schema, user, EPermission::Create);
    }

    std::unique_ptr<IAttributeDictionary> attributeHolder;
    if (auto* attributeSet = schema->GetAttributes()) {
        attributeHolder = CreateEphemeralAttributes();
        for (const auto& pair : attributeSet->Attributes()) {
            attributeHolder->SetYson(pair.first, pair.second);
        }
        if (attributes) {
            auto attributeMap = PatchNode(attributeHolder->ToMap(), attributes->ToMap());
            attributeHolder = IAttributeDictionary::FromMap(attributeMap->AsMap());
        }
        attributes = attributeHolder.get();
    } else if (!attributes) {
        attributeHolder = CreateEphemeralAttributes();
        attributes = attributeHolder.get();
    }

    // ITypeHandler::CreateObject may modify the attributes.
    std::unique_ptr<IAttributeDictionary> replicatedAttributes;
    if (replicate) {
        replicatedAttributes = attributes->Clone();
    }

    auto* object = handler->CreateObject(hintId, attributes);

    if (Bootstrap_->IsMulticell() && Any(flags & ETypeFlags::TwoPhaseCreation)) {
        object->SetLifeStage(EObjectLifeStage::CreationStarted);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Two-phase object creation started (ObjectId: %v)",
            object->GetId());
    } else {
        object->SetLifeStage(EObjectLifeStage::CreationCommitted);
    }

    YT_VERIFY(object->GetObjectRefCounter() == 1);

    if (CellTagFromId(object->GetId()) != Bootstrap_->GetCellTag()) {
        object->SetForeign();
    }

    try {
        FillAttributes(object, *attributes);
    } catch (const std::exception& ex) {
        UnrefObject(object);
        throw;
    }

    auto* acd = securityManager->FindAcd(object);
    if (acd) {
        acd->SetOwner(user);
    }

    if (replicate) {
        NProto::TReqCreateForeignObject replicationRequest;
        ToProto(replicationRequest.mutable_object_id(), object->GetId());
        replicationRequest.set_type(static_cast<int>(type));
        ToProto(replicationRequest.mutable_object_attributes(), *replicatedAttributes);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto replicationCellTags = handler->GetReplicationCellTags(object);
        multicellManager->PostToMasters(replicationRequest, replicationCellTags);
    }

    ConfirmObjectLifeStageToPrimaryMaster(object);

    return object;
}

void TObjectManager::ConfirmObjectLifeStageToPrimaryMaster(TObjectBase* object)
{
    if (!Bootstrap_->IsSecondaryMaster() || IsStableLifeStage(object->GetLifeStage())) {
        return;
    }

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Confirming object life stage to primary master (ObjectId: %v, LifeStage: %v)",
        object->GetId(),
        object->GetLifeStage());

    NProto::TReqConfirmObjectLifeStage request;
    ToProto(request.mutable_object_id(), object->GetId());
    request.set_cell_tag(Bootstrap_->GetCellTag());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(request, PrimaryMasterCellTag);
}

TObjectBase* TObjectManager::ResolvePathToObject(const TYPath& path, TTransaction* transaction)
{
    // Shortcut.
    if (path.empty()) {
        return GetMasterObject();
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    NYPath::TTokenizer tokenizer(path);

    auto doResolve = [&] (IObjectProxy* proxy) {
        // Shortcut.
        auto suffixPath = tokenizer.GetSuffix();
        if (suffixPath.empty()) {
            return proxy->GetObject();
        }

        // Slow path.
        auto req = TObjectYPathProxy::GetBasicAttributes(TYPath(suffixPath));
        SetTransactionId(req, GetObjectId(transaction));
        auto rsp = SyncExecuteVerb(proxy, req);
        auto objectId = FromProto<TObjectId>(rsp->object_id());
        return GetObjectOrThrow(objectId);
    };

    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::EndOfStream:
            return GetMasterObject();

        case NYPath::ETokenType::Slash: {
            auto root = cypressManager->GetNodeProxy(
                cypressManager->GetRootNode(),
                transaction);
            return doResolve(root.Get());
        }

        case NYPath::ETokenType::Literal: {
            const auto& token = tokenizer.GetToken();
            if (!token.StartsWith(ObjectIdPathPrefix)) {
                tokenizer.ThrowUnexpected();
            }

            TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
            TObjectId objectId;
            if (!TObjectId::FromString(objectIdString, &objectId)) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Error parsing object id %Qv",
                    objectIdString);
            }

            auto* object = objectManager->GetObjectOrThrow(objectId);
            auto proxy = objectManager->GetProxy(object, transaction);
            return doResolve(proxy.Get());
        }

        default:
            tokenizer.ThrowUnexpected();
            YT_ABORT();
    }
}

void TObjectManager::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
{
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    auto getPrerequisiteTransaction = [&] (TTransactionId transactionId) {
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is missing",
                transactionId);
        }
        if (transaction->GetPersistentState() != ETransactionState::Active) {
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
        ui64 revision = prerequisite.revision();

        auto* transaction = transactionId
            ? getPrerequisiteTransaction(transactionId)
            : nullptr;

        TCypressNodeBase* trunkNode;
        try {
            trunkNode = cypressManager->ResolvePathToTrunkNode(path, transaction);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: failed to resolve path %v",
                path)
                << ex;
        }

        if (trunkNode->GetRevision() != revision) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: node %v revision mismatch: expected %v, found %v",
                path,
                revision,
                trunkNode->GetRevision());
        }
    }
}

TFuture<TSharedRefArray> TObjectManager::ForwardObjectRequest(
    TSharedRefArray requestMessage,
    TCellTag cellTag,
    EPeerKind peerKind,
    std::optional<TDuration> timeout)
{
    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(ParseRequestHeader(requestMessage, &header));

    auto requestId = FromProto<TRequestId>(header.request_id());
    const auto& ypathExt = header.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto channel = multicellManager->GetMasterChannelOrThrow(cellTag, peerKind);

    TObjectServiceProxy proxy(std::move(channel));
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetTimeout(timeout);
    batchReq->SetUser(user->GetName());
    // NB: since single-subrequest batches are never backed off, this flag will
    // have no effect. Still, let's keep it correct just in case.
    bool needsSettingRetry = !header.retry() && FromProto<TMutationId>(header.mutation_id());
    batchReq->AddRequestMessage(requestMessage, needsSettingRetry);

    YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v:%v, Path: %v, Mutating: %v, "
        "CellTag: %v, PeerKind: %v)",
        requestId,
        batchReq->GetRequestId(),
        header.service(),
        header.method(),
        ypathExt.path(),
        ypathExt.mutating(),
        cellTag,
        peerKind);

    return batchReq->Invoke().Apply(BIND([=] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Object request forwarding failed");

        YT_LOG_DEBUG("Object request forwarding succeeded (RequestId: %v)",
            requestId);

        const auto& batchRsp = batchRspOrError.Value();
        return batchRsp->GetResponseMessage(0);
    }));
}

void TObjectManager::ReplicateObjectCreationToSecondaryMaster(
    TObjectBase* object,
    TCellTag cellTag)
{
    ReplicateObjectCreationToSecondaryMasters(object, {cellTag});
}

void TObjectManager::ReplicateObjectCreationToSecondaryMasters(
    TObjectBase* object,
    const TCellTagList& cellTags)
{
    if (cellTags.empty()) {
        return;
    }

    if (object->IsBuiltin()) {
        return;
    }

    NProto::TReqCreateForeignObject request;
    ToProto(request.mutable_object_id(), object->GetId());
    request.set_type(static_cast<int>(object->GetType()));
    ToProto(request.mutable_object_attributes(), *GetReplicatedAttributes(object, true));

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMasters(request, cellTags);
}

void TObjectManager::ReplicateObjectAttributesToSecondaryMaster(
    TObjectBase* object,
    TCellTag cellTag)
{
    auto req = TYPathProxy::Set(FromObjectId(object->GetId()) + "/@");
    req->set_value(ConvertToYsonString(GetReplicatedAttributes(object, false)->ToMap()).GetData());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(req, cellTag);
}

TString TObjectManager::MakeCodicilData(const TString& userName)
{
    return Format("User: %v", userName);
}

void TObjectManager::HydraExecuteLeader(
    const TString& userName,
    const TString& codicilData,
    const IServiceContextPtr& rpcContext,
    TMutationContext* mutationContext)
{
    TWallTimer timer;

    TCodicilGuard codicilGuard(codicilData);

    const auto& securityManager = Bootstrap_->GetSecurityManager();

    TUser* user = nullptr;
    try {
        user = securityManager->GetUserByNameOrThrow(userName);
        TAuthenticatedUserGuard userGuard(securityManager, user);
        ExecuteVerb(RootService_, rpcContext);
    } catch (const std::exception& ex) {
        rpcContext->Reply(ex);
    }

    if (!IsRecovery() && IsObjectAlive(user)) {
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, timer.GetElapsedTime()});
    }

    auto mutationId = rpcContext->GetMutationId();
    if (mutationId && !mutationContext->GetResponseKeeperSuppressed()) {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& responseKeeper = hydraFacade->GetResponseKeeper();
        // NB: Context must already be replied by now.
        responseKeeper->EndRequest(mutationId, rpcContext->GetResponseMessage());
    }
}

void TObjectManager::HydraExecuteFollower(NProto::TReqExecute* request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TSharedRefArrayBuilder requestMessageBuilder(request->request_parts_size());
    for (const auto& part : request->request_parts()) {
        // NB: This is a mutating request and these are always synchronous;
        // non-owning shared ref should be OK.
        requestMessageBuilder.Add(TSharedRef(TRef::FromString(part), nullptr));
    }

    auto requestMessage = requestMessageBuilder.Finish();

    auto context = CreateYPathContext(
        std::move(requestMessage),
        ObjectServerLogger,
        NLogging::ELogLevel::Debug);

    const auto& userName = request->user_name();
    auto codicilData = MakeCodicilData(userName);

    HydraExecuteLeader(userName, codicilData, context, GetCurrentMutationContext());
}

void TObjectManager::HydraDestroyObjects(NProto::TReqDestroyObjects* request)
{
    // NB: Ordered map is a must to make the behavior deterministic.
    std::map<TCellTag, NProto::TReqUnrefExportedObjects> crossCellRequestMap;
    auto getCrossCellRequest = [&] (TObjectId id) -> NProto::TReqUnrefExportedObjects& {
        return crossCellRequestMap[CellTagFromId(id)];
    };

    for (const auto& protoId : request->object_ids()) {
        auto id = FromProto<TObjectId>(protoId);
        auto type = TypeFromId(id);

        const auto& handler = GetHandler(type);
        auto* object = handler->FindObject(id);

        if (!object || object->GetObjectRefCounter() > 0)
            continue;

        if (object->IsForeign() && object->GetImportRefCounter() > 0) {
            auto& crossCellRequest = getCrossCellRequest(id);
            crossCellRequest.set_cell_tag(Bootstrap_->GetCellTag());
            auto* entry = crossCellRequest.add_entries();
            ToProto(entry->mutable_object_id(), id);
            entry->set_import_ref_counter(object->GetImportRefCounter());
        }

        // NB: The order of Dequeue/Destroy/CheckEmpty calls matters.
        // CheckEmpty will raise CollectPromise_ when GC queue becomes empty.
        // To enable cascaded GC sweep we don't want this to happen
        // if some ids are added during DestroyObject.
        GarbageCollector_->DestroyZombie(object);
        ++DestroyedObjects_;

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %v, Id: %v)",
            type,
            id);
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    for (const auto& pair : crossCellRequestMap) {
        auto cellTag = pair.first;
        const auto& perCellRequest = pair.second;
        multicellManager->PostToMaster(perCellRequest, cellTag);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Requesting to unreference imported objects (CellTag: %v, Count: %v)",
            cellTag,
            perCellRequest.entries_size());
    }

    GarbageCollector_->CheckEmpty();
}

void TObjectManager::HydraCreateForeignObject(NProto::TReqCreateForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());
    auto type = EObjectType(request->type());

    auto attributes = request->has_object_attributes()
        ? FromProto(request->object_attributes())
        : std::unique_ptr<IAttributeDictionary>();

    CreateObject(
        objectId,
        type,
        attributes.get());

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Foreign object created (Id: %v, Type: %v)",
        objectId,
        type);
}

void TObjectManager::HydraRemoveForeignObject(NProto::TReqRemoveForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());

    auto* object = FindObject(objectId);
    if (object) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Removing foreign object (ObjectId: %v, RefCounter: %v)",
            objectId,
            object->GetObjectRefCounter());
        UnrefObject(object);
    } else {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Attempt to remove a non-existing foreign object (ObjectId: %v)",
            objectId);
    }
}

void TObjectManager::HydraUnrefExportedObjects(NProto::TReqUnrefExportedObjects* request) noexcept
{
    auto cellTag = request->cell_tag();

    for (const auto& entry : request->entries()) {
        auto objectId = FromProto<TObjectId>(entry.object_id());
        auto importRefCounter = entry.import_ref_counter();

        auto* object = GetObject(objectId);
        UnrefObject(object, importRefCounter);

        const auto& handler = GetHandler(object);
        handler->UnexportObject(object, cellTag, importRefCounter);
    }

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Exported objects unreferenced (CellTag: %v, Count: %v)",
        cellTag,
        request->entries_size());
}

void TObjectManager::HydraConfirmObjectLifeStage(NProto::TReqConfirmObjectLifeStage* confirmRequest) noexcept
{
    YT_VERIFY(Bootstrap_->IsPrimaryMaster());

    auto objectId = FromProto<TObjectId>(confirmRequest->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "A non-existing object creation confirmed by a secondary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto voteCount = object->IncrementLifeStageVoteCount();
    YT_VERIFY(voteCount <= Bootstrap_->GetSecondaryCellTags().size());

    auto oldLifeStage = object->GetLifeStage();

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object life stage confirmed by secondary master (ObjectId: %v, CellTag: %v, LifeStage: %v, VoteCount: %v)",
        objectId,
        confirmRequest->cell_tag(),
        oldLifeStage,
        voteCount);

    if (voteCount != Bootstrap_->GetSecondaryCellTags().size()) {
        return;
    }

    auto newLifeStage = GetNextLifeStage(oldLifeStage);
    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object votes collected; advancing life stage (ObjectId: %v, LifeStage: %v -> %v)",
        objectId,
        oldLifeStage,
        newLifeStage);

    NProto::TReqAdvanceObjectLifeStage advanceRequest;
    ToProto(advanceRequest.mutable_object_id(), object->GetId());
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToSecondaryMasters(advanceRequest);
}

void TObjectManager::HydraAdvanceObjectLifeStage(NProto::TReqAdvanceObjectLifeStage* request) noexcept
{
    YT_VERIFY(Bootstrap_->IsSecondaryMaster());

    auto objectId = FromProto<TObjectId>(request->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(),
            "Life stage advancement for a non-existing object requested by the primary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto oldLifeStage = object->GetLifeStage();
    auto newLifeStage = GetNextLifeStage(oldLifeStage);
    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object life stage advanced by primary master (ObjectId: %v, LifeStage: %v -> %v)",
        objectId,
        oldLifeStage,
        newLifeStage);

    ConfirmObjectLifeStageToPrimaryMaster(object);
}

const TProfiler& TObjectManager::GetProfiler()
{
    return Profiler;
}

NProfiling::TMonotonicCounter* TObjectManager::GetMethodCumulativeExecuteTimeCounter(
    EObjectType type,
    const TString& method)
{
    auto key = std::make_pair(type, method);
    auto it = MethodToEntry_.find(key);
    if (it == MethodToEntry_.end()) {
        auto entry = std::make_unique<TMethodEntry>();
        entry->CumulativeExecuteTimeCounter = NProfiling::TMonotonicCounter(
            "/cumulative_execute_time",
            {
                TProfileManager::Get()->RegisterTag("type", type),
                TProfileManager::Get()->RegisterTag("method", method)
            });
        it = MethodToEntry_.emplace(key, std::move(entry)).first;
    }
    return &it->second->CumulativeExecuteTimeCounter;
}

TEpoch TObjectManager::GetCurrentEpoch()
{
    return CurrentEpoch_;
}

void TObjectManager::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Profiler.Enqueue("/zombie_object_count", GarbageCollector_->GetZombieCount(), EMetricType::Gauge);
    Profiler.Enqueue("/ephemeral_ghost_object_count", GarbageCollector_->GetEphemeralGhostCount(), EMetricType::Gauge);
    Profiler.Enqueue("/weak_ghost_object_count", GarbageCollector_->GetWeakGhostCount(), EMetricType::Gauge);
    Profiler.Enqueue("/locked_object_count", GarbageCollector_->GetLockedCount(), EMetricType::Gauge);
    Profiler.Enqueue("/created_objects", CreatedObjects_, EMetricType::Counter);
    Profiler.Enqueue("/destroyed_objects", DestroyedObjects_, EMetricType::Counter);
}

std::unique_ptr<NYTree::IAttributeDictionary> TObjectManager::GetReplicatedAttributes(
    TObjectBase* object,
    bool mandatory)
{
    YT_VERIFY(!IsVersionedType(object->GetType()));

    const auto& handler = GetHandler(object);
    auto proxy = handler->GetProxy(object, nullptr);

    auto attributes = CreateEphemeralAttributes();
    THashSet<TString> replicatedKeys;
    auto replicateKey = [&] (const TString& key, const TYsonString& value) {
        if (replicatedKeys.insert(key).second) {
            attributes->SetYson(key, value);
        }
    };

    // Check system attributes.
    std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
    proxy->ListBuiltinAttributes(&descriptors);
    for (const auto& descriptor : descriptors) {
        if (!descriptor.Replicated) {
            continue;
        }

        if (descriptor.Mandatory != mandatory) {
            continue;
        }

        auto value = proxy->FindBuiltinAttribute(descriptor.InternedKey);
        if (value) {
            const auto& key = GetUninternedAttributeKey(descriptor.InternedKey);
            replicateKey(key, value);
        }
    }

    // Check custom attributes.
    const auto* customAttributes = object->GetAttributes();
    if (customAttributes) {
        for (const auto& pair : object->GetAttributes()->Attributes()) {
            replicateKey(pair.first, pair.second);
        }
    }
    return attributes;
}

void TObjectManager::OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
{
    auto schemas = GetValuesSortedByKey(SchemaMap_);
    for (auto* schema : schemas) {
        ReplicateObjectAttributesToSecondaryMaster(schema, cellTag);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

