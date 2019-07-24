#include "object_manager.h"
#include "private.h"
#include "config.h"
#include "garbage_collector.h"
#include "master.h"
#include "master_type_handler.h"
#include "schema.h"
#include "type_handler.h"
#include "path_resolver.h"

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/cell_master/automaton.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_list.h>

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/node_detail.h>

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

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/core/erasure/public.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/ytree/node_detail.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/misc/crash_handler.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

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

class TObjectManager::TImpl
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    void RegisterHandler(IObjectTypeHandlerPtr handler);
    const IObjectTypeHandlerPtr& FindHandler(EObjectType type) const;
    const IObjectTypeHandlerPtr& GetHandler(EObjectType type) const;
    const IObjectTypeHandlerPtr& GetHandler(const TObject* object) const;

    const std::set<EObjectType>& GetRegisteredTypes() const;

    TObjectId GenerateId(EObjectType type, TObjectId hintId);

    int RefObject(TObject* object);
    int UnrefObject(TObject* object, int count = 1);
    int GetObjectRefCounter(TObject* object);
    int EphemeralRefObject(TObject* object);
    int EphemeralUnrefObject(TObject* object);
    int GetObjectEphemeralRefCounter(TObject* object);
    int WeakRefObject(TObject* object);
    int WeakUnrefObject(TObject* object);
    int GetObjectWeakRefCounter(TObject* object);

    TObject* FindObject(TObjectId id);
    TObject* GetObject(TObjectId id);
    TObject* GetObjectOrThrow(TObjectId id);
    TObject* GetWeakGhostObject(TObjectId id);
    void RemoveObject(TObject* object);

    IYPathServicePtr CreateRemoteProxy(TObjectId id);

    IObjectProxyPtr GetProxy(
        TObject* object,
        TTransaction* transaction = nullptr);

    void BranchAttributes(
        const TObject* originatingObject,
        TObject* branchedObject);
    void MergeAttributes(
        TObject* originatingObject,
        const TObject* branchedObject);
    void FillAttributes(
        TObject* object,
        const IAttributeDictionary& attributes);

    IYPathServicePtr GetRootService();

    TObject* GetMasterObject();
    IObjectProxyPtr GetMasterProxy();

    TObject* FindSchema(EObjectType type);
    TObject* GetSchema(EObjectType type);
    IObjectProxyPtr GetSchemaProxy(EObjectType type);

    std::unique_ptr<TMutation> CreateExecuteMutation(
        const TString& userName,
        const IServiceContextPtr& context);
    std::unique_ptr<TMutation> CreateDestroyObjectsMutation(
        const NProto::TReqDestroyObjects& request);

    TFuture<void> GCCollect();

    TObject* CreateObject(
        TObjectId hintId,
        EObjectType type,
        IAttributeDictionary* attributes);

    TObject* ResolvePathToObject(
        const TYPath& path,
        TTransaction* transaction);

    void ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites);

    TFuture<TSharedRefArray> ForwardObjectRequest(
        TSharedRefArray requestMessage,
        TCellTag cellTag,
        EPeerKind peerKind,
        std::optional<TDuration> timeout);

    void ReplicateObjectCreationToSecondaryMaster(
        TObject* object,
        TCellTag cellTag);

    void ReplicateObjectCreationToSecondaryMasters(
        TObject* object,
        const TCellTagList& cellTags);

    void ReplicateObjectAttributesToSecondaryMaster(
        TObject* object,
        TCellTag cellTag);

    const NProfiling::TProfiler& GetProfiler();
    NProfiling::TMonotonicCounter* GetMethodCumulativeExecuteTimeCounter(EObjectType type, const TString& method);

    TEpoch GetCurrentEpoch();

private:
    friend class TObjectProxyBase;

    class TRootService;
    using TRootServicePtr = TIntrusivePtr<TRootService>;
    class TRemoteProxy;

    NProfiling::TProfiler Profiler;

    struct TTypeEntry
    {
        IObjectTypeHandlerPtr Handler;
        TSchemaObject* SchemaObject = nullptr;
        IObjectProxyPtr SchemaProxy;
    };

    std::set<EObjectType> RegisteredTypes_;
    THashMap<EObjectType, TTypeEntry> TypeToEntry_;

    struct TMethodEntry
    {
        NProfiling::TMonotonicCounter CumulativeExecuteTimeCounter;
    };

    THashMap<std::pair<EObjectType, TString>, std::unique_ptr<TMethodEntry>> MethodToEntry_;

    TRootServicePtr RootService_;

    TObjectId MasterObjectId_;
    std::unique_ptr<TMasterObject> MasterObject_;

    IObjectProxyPtr MasterProxy_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    TGarbageCollectorPtr GarbageCollector_;

    int CreatedObjects_ = 0;
    int DestroyedObjects_ = 0;

    //! Stores schemas (for serialization mostly).
    TEntityMap<TSchemaObject> SchemaMap_;

    TEpoch CurrentEpoch_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;

    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);

    virtual void OnRecoveryStarted() override;
    virtual void OnRecoveryComplete() override;
    virtual void Clear() override;
    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    static TString MakeCodicilData(const TString& userName);
    void HydraExecuteLeader(
        const TString& userName,
        const TString& codicilData,
        const IServiceContextPtr& rpcContext,
        TMutationContext*);
    void HydraExecuteFollower(NProto::TReqExecute* request);
    void HydraDestroyObjects(NProto::TReqDestroyObjects* request);
    void HydraCreateForeignObject(NProto::TReqCreateForeignObject* request) noexcept;
    void HydraRemoveForeignObject(NProto::TReqRemoveForeignObject* request) noexcept;
    void HydraUnrefExportedObjects(NProto::TReqUnrefExportedObjects* request) noexcept;
    void HydraConfirmObjectLifeStage(NProto::TReqConfirmObjectLifeStage* request) noexcept;
    void HydraAdvanceObjectLifeStage(NProto::TReqAdvanceObjectLifeStage* request) noexcept;

    void DoRemoveObject(TObject* object);
    void CheckRemovingObjectRefCounter(TObject* object);
    void CheckObjectLifeStageVoteCount(TObject* object);
    void ConfirmObjectLifeStageToPrimaryMaster(TObject* object);

    void OnProfiling();

    std::unique_ptr<IAttributeDictionary> GetReplicatedAttributes(
        TObject* object,
        bool mandatory);
    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag);

    void InitSchemas();

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TImpl::TRemoteProxy
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

class TObjectManager::TImpl::TRootService
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

TObjectManager::TImpl::TImpl(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ObjectManager)
    , Profiler(ObjectServerProfiler)
    , RootService_(New<TRootService>(Bootstrap_))
    , GarbageCollector_(New<TGarbageCollector>(Bootstrap_))
{
    YT_VERIFY(bootstrap);

    RegisterLoader(
        "ObjectManager.Keys",
        BIND(&TImpl::LoadKeys, Unretained(this)));
    RegisterLoader(
        "ObjectManager.Values",
        BIND(&TImpl::LoadValues, Unretained(this)));

    RegisterSaver(
        ESyncSerializationPriority::Keys,
        "ObjectManager.Keys",
        BIND(&TImpl::SaveKeys, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "ObjectManager.Values",
        BIND(&TImpl::SaveValues, Unretained(this)));

    RegisterHandler(CreateMasterTypeHandler(Bootstrap_));

    RegisterMethod(BIND(&TImpl::HydraExecuteFollower, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraDestroyObjects, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraCreateForeignObject, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraRemoveForeignObject, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraUnrefExportedObjects, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraConfirmObjectLifeStage, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraAdvanceObjectLifeStage, Unretained(this)));

    MasterObjectId_ = MakeWellKnownId(EObjectType::Master, Bootstrap_->GetPrimaryCellTag());
}

void TObjectManager::TImpl::Initialize()
{
    if (Bootstrap_->IsPrimaryMaster()) {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->SubscribeReplicateValuesToSecondaryMaster(
            BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
    }

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TImpl::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor_->Start();
}

IYPathServicePtr TObjectManager::TImpl::GetRootService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootService_;
}

TObject* TObjectManager::TImpl::GetMasterObject()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterObject_.get();
}

IObjectProxyPtr TObjectManager::TImpl::GetMasterProxy()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterProxy_;
}

TObject* TObjectManager::TImpl::FindSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto it = TypeToEntry_.find(type);
    return it == TypeToEntry_.end() ? nullptr : it->second.SchemaObject;
}

TObject* TObjectManager::GetSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* schema = FindSchema(type);
    YT_VERIFY(schema);
    return schema;
}

IObjectProxyPtr TObjectManager::TImpl::GetSchemaProxy(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto it = TypeToEntry_.find(type);
    YT_VERIFY(it != TypeToEntry_.end());
    const auto& entry = it->second;
    YT_VERIFY(entry.SchemaProxy);
    return entry.SchemaProxy;
}

void TObjectManager::TImpl::RegisterHandler(IObjectTypeHandlerPtr handler)
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

const IObjectTypeHandlerPtr& TObjectManager::TImpl::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto it = TypeToEntry_.find(type);
    return it == TypeToEntry_.end() ? NullTypeHandler : it->second.Handler;
}

const IObjectTypeHandlerPtr& TObjectManager::TImpl::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& handler = FindHandler(type);
    YT_ASSERT(handler);
    return handler;
}

const IObjectTypeHandlerPtr& TObjectManager::TImpl::GetHandler(const TObject* object) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetHandler(object->GetType());
}

const std::set<EObjectType>& TObjectManager::TImpl::GetRegisteredTypes() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RegisteredTypes_;
}

TObjectId TObjectManager::TImpl::GenerateId(EObjectType type, TObjectId hintId)
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

int TObjectManager::TImpl::RefObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!object->IsDestroyed());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object referenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectEphemeralRefCounter(object),
        GetObjectWeakRefCounter(object));

    if (refCounter == 1) {
        GarbageCollector_->UnregisterZombie(object);
        if (object->GetLifeStage() == EObjectLifeStage::RemovalPreCommitted || object->GetLifeStage() == EObjectLifeStage::RemovalCommitted) {
            YT_LOG_ERROR_UNLESS(IsRecovery(), "Unexpected error: references to object reappeared after removal "
                "has been pre-committed (ObjectId: %v, LifeStage: %v)",
                object->GetId(),
                object->GetLifeStage());
        }
    }

    return refCounter;
}

int TObjectManager::TImpl::UnrefObject(TObject* object, int count)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->IsAlive());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject(count);
    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object unreferenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectEphemeralRefCounter(object),
        GetObjectWeakRefCounter(object));

    if (refCounter == 0) {
        const auto& handler = GetHandler(object);
        handler->ZombifyObject(object);

        GarbageCollector_->RegisterZombie(object);

        auto flags = handler->GetFlags();
        if (Any(flags & ETypeFlags::ReplicateDestroy) &&
            None(flags & ETypeFlags::TwoPhaseRemoval))
        {
            NProto::TReqRemoveForeignObject request;
            ToProto(request.mutable_object_id(), object->GetId());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            auto replicationCellTags = handler->GetReplicationCellTags(object);
            multicellManager->PostToMasters(request, replicationCellTags);
        }
    }

    if (object->GetLifeStage() == EObjectLifeStage::RemovalStarted) {
        CheckRemovingObjectRefCounter(object);
    }

    return refCounter;
}

int TObjectManager::TImpl::GetObjectRefCounter(TObject* object)
{
    return object->GetObjectRefCounter();
}

int TObjectManager::TImpl::EphemeralRefObject(TObject* object)
{
    return GarbageCollector_->EphemeralRefObject(object, CurrentEpoch_);
}

int TObjectManager::TImpl::EphemeralUnrefObject(TObject* object)
{
    return GarbageCollector_->EphemeralUnrefObject(object, CurrentEpoch_);
}

int TObjectManager::TImpl::GetObjectEphemeralRefCounter(TObject* object)
{
    return object->GetObjectEphemeralRefCounter(CurrentEpoch_);
}

int TObjectManager::TImpl::WeakRefObject(TObject* object)
{
    return GarbageCollector_->WeakRefObject(object, CurrentEpoch_);
}

int TObjectManager::TImpl::WeakUnrefObject(TObject* object)
{
    return GarbageCollector_->WeakUnrefObject(object, CurrentEpoch_);
}

int TObjectManager::TImpl::GetObjectWeakRefCounter(TObject* object)
{
    return object->GetObjectWeakRefCounter();
}

void TObjectManager::TImpl::SaveKeys(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveKeys(context);
    GarbageCollector_->SaveKeys(context);
}

void TObjectManager::TImpl::SaveValues(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveValues(context);
    GarbageCollector_->SaveValues(context);
}

void TObjectManager::TImpl::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadKeys(context);
    GarbageCollector_->LoadKeys(context);
}

void TObjectManager::TImpl::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadValues(context);

    InitSchemas();

    GarbageCollector_->LoadValues(context);
}

void TObjectManager::TImpl::Clear()
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

void TObjectManager::TImpl::InitSchemas()
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

void TObjectManager::TImpl::OnRecoveryStarted()
{
    Profiler.SetEnabled(false);

    ++CurrentEpoch_;
    GarbageCollector_->Reset();
}

void TObjectManager::TImpl::OnRecoveryComplete()
{
    Profiler.SetEnabled(true);
}

void TObjectManager::TImpl::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector_->Start();
}

void TObjectManager::TImpl::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector_->Stop();
}

TObject* TObjectManager::TImpl::FindObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->FindObject(id);
}

TObject* TObjectManager::TImpl::GetObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* object = FindObject(id);
    YT_VERIFY(object);
    return object;
}

TObject* TObjectManager::TImpl::GetObjectOrThrow(TObjectId id)
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

TObject* TObjectManager::TImpl::GetWeakGhostObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    return GarbageCollector_->GetWeakGhostObject(id);
}

void TObjectManager::TImpl::RemoveObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (object->GetLifeStage() != EObjectLifeStage::CreationCommitted) {
        THROW_ERROR_EXCEPTION("Object life stage is %Qlv",
            object->GetLifeStage());
    }

    const auto& handler = GetHandler(object);
    if (None(handler->GetFlags() & ETypeFlags::Removable)) {
        THROW_ERROR_EXCEPTION("Object cannot be removed");
    }

    if (object->IsForeign()) {
        THROW_ERROR_EXCEPTION("Object is foreign");
    }

    if (Any(handler->GetFlags() & ETypeFlags::TwoPhaseRemoval) && Bootstrap_->IsPrimaryMaster()) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Two-phase object removal started (ObjectId: %v, RefCounter: %v)",
            object->GetId(),
            object->GetObjectRefCounter());

        object->SetLifeStage(EObjectLifeStage::RemovalStarted);
        object->ResetLifeStageVoteCount();

        NProto::TReqRemoveForeignObject request;
        ToProto(request.mutable_object_id(), object->GetId());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(request);

        CheckRemovingObjectRefCounter(object);
    } else {
        if (object->GetObjectRefCounter() != 1) {
            THROW_ERROR_EXCEPTION("Object is in use");
        }

        object->SetLifeStage(EObjectLifeStage::RemovalCommitted);
        DoRemoveObject(object);
    }
}

IYPathServicePtr TObjectManager::TImpl::CreateRemoteProxy(TObjectId id)
{
    return New<TRemoteProxy>(Bootstrap_, id);
}

IObjectProxyPtr TObjectManager::TImpl::GetProxy(
    TObject* object,
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

void TObjectManager::TImpl::BranchAttributes(
    const TObject* /*originatingObject*/,
    TObject* /*branchedObject*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    // We don't store empty deltas at the moment
}

void TObjectManager::TImpl::MergeAttributes(
    TObject* originatingObject,
    const TObject* branchedObject)
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

void TObjectManager::TImpl::FillAttributes(
    TObject* object,
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

std::unique_ptr<TMutation> TObjectManager::TImpl::CreateExecuteMutation(
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
        &TImpl::HydraExecuteLeader,
        MakeStrong(this),
        userName,
        MakeCodicilData(userName),
        context));
    return mutation;
}

std::unique_ptr<TMutation> TObjectManager::TImpl::CreateDestroyObjectsMutation(const NProto::TReqDestroyObjects& request)
{
    return CreateMutation(
        Bootstrap_->GetHydraFacade()->GetHydraManager(),
        request,
        &TImpl::HydraDestroyObjects,
        this);
}

TFuture<void> TObjectManager::TImpl::GCCollect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GarbageCollector_->Collect();
}

TObject* TObjectManager::TImpl::CreateObject(
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
        object->ResetLifeStageVoteCount();
        YT_VERIFY(object->IncrementLifeStageVoteCount() == 1);
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

void TObjectManager::TImpl::ConfirmObjectLifeStageToPrimaryMaster(TObject* object)
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

TObject* TObjectManager::TImpl::ResolvePathToObject(const TYPath& path, TTransaction* transaction)
{
    TPathResolver resolver(
        Bootstrap_,
        TString(),
        TString(),
        path,
        transaction,
        TPathResolverOptions{
            .EnablePartialResolve = false
        });

    auto result = resolver.Resolve();
    const auto* payload = std::get_if<TPathResolver::TLocalObjectPayload>(&result.Payload);
    if (!payload) {
        THROW_ERROR_EXCEPTION("%v is not a local object",
            path);
    }

    return payload->Object;
}

void TObjectManager::TImpl::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
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

        TCypressNode* trunkNode;
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

TFuture<TSharedRefArray> TObjectManager::TImpl::ForwardObjectRequest(
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

void TObjectManager::TImpl::ReplicateObjectCreationToSecondaryMaster(
    TObject* object,
    TCellTag cellTag)
{
    ReplicateObjectCreationToSecondaryMasters(object, {cellTag});
}

void TObjectManager::TImpl::ReplicateObjectCreationToSecondaryMasters(
    TObject* object,
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

void TObjectManager::TImpl::ReplicateObjectAttributesToSecondaryMaster(
    TObject* object,
    TCellTag cellTag)
{
    auto req = TYPathProxy::Set(FromObjectId(object->GetId()) + "/@");
    req->set_value(ConvertToYsonString(GetReplicatedAttributes(object, false)->ToMap()).GetData());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(req, cellTag);
}

TString TObjectManager::TImpl::MakeCodicilData(const TString& userName)
{
    return Format("User: %v", userName);
}

void TObjectManager::TImpl::HydraExecuteLeader(
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

void TObjectManager::TImpl::HydraExecuteFollower(NProto::TReqExecute* request)
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

void TObjectManager::TImpl::HydraDestroyObjects(NProto::TReqDestroyObjects* request)
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

void TObjectManager::TImpl::HydraCreateForeignObject(NProto::TReqCreateForeignObject* request) noexcept
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

void TObjectManager::TImpl::HydraRemoveForeignObject(NProto::TReqRemoveForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());

    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Attempt to remove a non-existing foreign object (ObjectId: %v)",
            objectId);
        return;
    }

    if (object->GetLifeStage() != EObjectLifeStage::CreationCommitted) {
        YT_LOG_ERROR_UNLESS(IsRecovery(),
            "Unexpected error: requested to remove a foreign object with inappropriate life stage (ObjectId: %v, LifeStage: %v)",
            objectId,
            object->GetLifeStage());
        return;
    }

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Removing foreign object (ObjectId: %v, RefCounter: %v)",
        objectId,
        object->GetObjectRefCounter());

    const auto& handler = GetHandler(object);
    if (Any(handler->GetFlags() & ETypeFlags::TwoPhaseRemoval)) {
        object->SetLifeStage(EObjectLifeStage::RemovalStarted);
        CheckRemovingObjectRefCounter(object);
    } else {
        object->SetLifeStage(EObjectLifeStage::RemovalCommitted);
        DoRemoveObject(object);
    }
}

void TObjectManager::TImpl::HydraUnrefExportedObjects(NProto::TReqUnrefExportedObjects* request) noexcept
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

void TObjectManager::TImpl::HydraConfirmObjectLifeStage(NProto::TReqConfirmObjectLifeStage* confirmRequest) noexcept
{
    YT_VERIFY(Bootstrap_->IsPrimaryMaster());

    auto objectId = FromProto<TObjectId>(confirmRequest->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "A non-existing object creation confirmed by secondary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto voteCount = object->IncrementLifeStageVoteCount();
    YT_LOG_DEBUG_UNLESS(IsRecovery(),
        "Object life stage confirmed by secondary cell (ObjectId: %v, CellTag: %v, VoteCount: %v)",
        objectId,
        confirmRequest->cell_tag(),
        voteCount);

    CheckObjectLifeStageVoteCount(object);
}

void TObjectManager::TImpl::HydraAdvanceObjectLifeStage(NProto::TReqAdvanceObjectLifeStage* request) noexcept
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

    if (!object->IsForeign()) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(),
            "Life stage advancement for a non-foreign object requested by primary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto oldLifeStage = object->GetLifeStage();
    auto newLifeStage = GetNextLifeStage(oldLifeStage);
    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG_UNLESS(IsRecovery(),
        "Object life stage advanced by primary cell (ObjectId: %v, LifeStage: %v -> %v)",
        objectId,
        oldLifeStage,
        newLifeStage);

    ConfirmObjectLifeStageToPrimaryMaster(object);

    if (newLifeStage == EObjectLifeStage::RemovalCommitted) {
        DoRemoveObject(object);
    }
}

void TObjectManager::TImpl::DoRemoveObject(TObject* object)
{
    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object removed (ObjectId: %v)",
        object->GetId());
    if (UnrefObject(object) != 0) {
        YT_LOG_ERROR_UNLESS(IsRecovery(),
            "Unexpected error: non-zero reference counter for after object removal (ObjectId: %v, RefCounter: %v)",
            object->GetId(),
            object->GetObjectRefCounter());
    }
}

void TObjectManager::TImpl::CheckRemovingObjectRefCounter(TObject* object)
{
    if (object->GetObjectRefCounter() != 1) {
        return;
    }

    auto oldLifeStage = object->GetLifeStage();
    auto newLifeStage = GetNextLifeStage(oldLifeStage);

    YT_VERIFY(oldLifeStage == EObjectLifeStage::RemovalStarted);
    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object references released (ObjectId: %v, LifeStage: %v -> %v)",
        object->GetId(),
        oldLifeStage,
        newLifeStage);

    if (Bootstrap_->IsPrimaryMaster()) {
        object->IncrementLifeStageVoteCount();
        CheckObjectLifeStageVoteCount(object);
    } else {
        ConfirmObjectLifeStageToPrimaryMaster(object);
    }
}

void TObjectManager::TImpl::CheckObjectLifeStageVoteCount(NYT::NObjectServer::TObject* object)
{
    while (true) {
        auto voteCount = object->GetLifeStageVoteCount();
        YT_VERIFY(voteCount <= Bootstrap_->GetSecondaryCellTags().size() + 1);
        if (voteCount < Bootstrap_->GetSecondaryCellTags().size() + 1) {
            break;
        }

        auto oldLifeStage = object->GetLifeStage();
        auto newLifeStage = GetNextLifeStage(oldLifeStage);
        object->SetLifeStage(newLifeStage);
        object->ResetLifeStageVoteCount();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Object life stage votes collected; advancing life stage (ObjectId: %v, LifeStage: %v -> %v)",
            object->GetId(),
            oldLifeStage,
            newLifeStage);

        NProto::TReqAdvanceObjectLifeStage advanceRequest;
        ToProto(advanceRequest.mutable_object_id(), object->GetId());
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToSecondaryMasters(advanceRequest);

        switch (newLifeStage) {
            case EObjectLifeStage::CreationPreCommitted:
                object->IncrementLifeStageVoteCount();
                break;

            case EObjectLifeStage::RemovalCommitted:
                DoRemoveObject(object);
                break;

            default:
                break;
        }
    }
}

const TProfiler& TObjectManager::TImpl::GetProfiler()
{
    return Profiler;
}

NProfiling::TMonotonicCounter* TObjectManager::TImpl::GetMethodCumulativeExecuteTimeCounter(
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

TEpoch TObjectManager::TImpl::GetCurrentEpoch()
{
    return CurrentEpoch_;
}

void TObjectManager::TImpl::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Profiler.Enqueue("/zombie_object_count", GarbageCollector_->GetZombieCount(), EMetricType::Gauge);
    Profiler.Enqueue("/ephemeral_ghost_object_count", GarbageCollector_->GetEphemeralGhostCount(), EMetricType::Gauge);
    Profiler.Enqueue("/weak_ghost_object_count", GarbageCollector_->GetWeakGhostCount(), EMetricType::Gauge);
    Profiler.Enqueue("/locked_object_count", GarbageCollector_->GetLockedCount(), EMetricType::Gauge);
    Profiler.Enqueue("/created_objects", CreatedObjects_, EMetricType::Counter);
    Profiler.Enqueue("/destroyed_objects", DestroyedObjects_, EMetricType::Counter);
}

std::unique_ptr<IAttributeDictionary> TObjectManager::TImpl::GetReplicatedAttributes(
    TObject* object,
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

void TObjectManager::TImpl::OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
{
    auto schemas = GetValuesSortedByKey(SchemaMap_);
    for (auto* schema : schemas) {
        ReplicateObjectAttributesToSecondaryMaster(schema, cellTag);
    }
}

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TObjectManager::Initialize()
{
    Impl_->Initialize();
}

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    Impl_->RegisterHandler(std::move(handler));
}

const IObjectTypeHandlerPtr& TObjectManager::FindHandler(EObjectType type) const
{
    return Impl_->FindHandler(type);
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(const TObject* object) const
{
    return Impl_->GetHandler(object);
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(EObjectType type) const
{
    return Impl_->GetHandler(type);
}

const std::set<EObjectType>& TObjectManager::GetRegisteredTypes() const
{
    return Impl_->GetRegisteredTypes();
}

TObjectId TObjectManager::GenerateId(EObjectType type, TObjectId hintId)
{
    return Impl_->GenerateId(type, hintId);
}

int TObjectManager::RefObject(TObject* object)
{
    return Impl_->RefObject(object);
}

int TObjectManager::UnrefObject(TObject* object, int count)
{
    return Impl_->UnrefObject(object, count);
}

int TObjectManager::GetObjectRefCounter(TObject* object)
{
    return Impl_->GetObjectRefCounter(object);
}

int TObjectManager::EphemeralRefObject(TObject* object)
{
    return Impl_->EphemeralRefObject(object);
}

int TObjectManager::EphemeralUnrefObject(TObject* object)
{
    return Impl_->EphemeralUnrefObject(object);
}

int TObjectManager::GetObjectEphemeralRefCounter(TObject* object)
{
    return Impl_->GetObjectEphemeralRefCounter(object);
}

int TObjectManager::WeakRefObject(TObject* object)
{
    return Impl_->WeakRefObject(object);
}

int TObjectManager::WeakUnrefObject(TObject* object)
{
    return Impl_->WeakUnrefObject(object);
}

int TObjectManager::GetObjectWeakRefCounter(TObject* object)
{
    return Impl_->GetObjectWeakRefCounter(object);
}

TObject* TObjectManager::FindObject(TObjectId id)
{
    return Impl_->FindObject(id);
}

TObject* TObjectManager::GetObject(TObjectId id)
{
    return Impl_->GetObject(id);
}

TObject* TObjectManager::GetObjectOrThrow(TObjectId id)
{
    return Impl_->GetObjectOrThrow(id);
}

TObject* TObjectManager::GetWeakGhostObject(TObjectId id)
{
    return Impl_->GetWeakGhostObject(id);
}

void TObjectManager::RemoveObject(TObject* object)
{
    Impl_->RemoveObject(object);
}

IYPathServicePtr TObjectManager::CreateRemoteProxy(TObjectId id)
{
    return Impl_->CreateRemoteProxy(id);
}

IObjectProxyPtr TObjectManager::GetProxy(TObject* object, TTransaction* transaction)
{
    return Impl_->GetProxy(object, transaction);
}

void TObjectManager::BranchAttributes(const TObject* originatingObject, TObject* branchedObject)
{
    Impl_->BranchAttributes(originatingObject, branchedObject);
}

void TObjectManager::MergeAttributes(TObject* originatingObject, const TObject* branchedObject)
{
    Impl_->MergeAttributes(originatingObject, branchedObject);
}

void TObjectManager::FillAttributes(TObject* object, const IAttributeDictionary& attributes)
{
    Impl_->FillAttributes(object, attributes);
}

IYPathServicePtr TObjectManager::GetRootService()
{
    return Impl_->GetRootService();
}

TObject* TObjectManager::GetMasterObject()
{
    return Impl_->GetMasterObject();
}

IObjectProxyPtr TObjectManager::GetMasterProxy()
{
    return Impl_->GetMasterProxy();
}

TObject* TObjectManager::FindSchema(EObjectType type)
{
    return Impl_->FindSchema(type);
}

IObjectProxyPtr TObjectManager::GetSchemaProxy(EObjectType type)
{
    return Impl_->GetSchemaProxy(type);
}

std::unique_ptr<TMutation> TObjectManager::CreateExecuteMutation(const TString& userName, const IServiceContextPtr& context)
{
    return Impl_->CreateExecuteMutation(userName, context);
}

std::unique_ptr<TMutation> TObjectManager::CreateDestroyObjectsMutation(const NProto::TReqDestroyObjects& request)
{
    return Impl_->CreateDestroyObjectsMutation(request);
}

TFuture<void> TObjectManager::GCCollect()
{
    return Impl_->GCCollect();
}

TObject* TObjectManager::CreateObject(TObjectId hintId, EObjectType type, IAttributeDictionary* attributes)
{
    return Impl_->CreateObject(hintId, type, attributes);
}

TObject* TObjectManager::ResolvePathToObject(const TYPath& path, TTransaction* transaction)
{
    return Impl_->ResolvePathToObject(path, transaction);
}

void TObjectManager::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
{
    Impl_->ValidatePrerequisites(prerequisites);
}

TFuture<TSharedRefArray> TObjectManager::ForwardObjectRequest(
    TSharedRefArray requestMessage,
    TCellTag cellTag,
    EPeerKind peerKind,
    std::optional<TDuration> timeout)
{
    return Impl_->ForwardObjectRequest(std::move(requestMessage), cellTag, peerKind, timeout);
}

void TObjectManager::ReplicateObjectCreationToSecondaryMaster(TObject* object, TCellTag cellTag)
{
    Impl_->ReplicateObjectCreationToSecondaryMaster(object, cellTag);
}

void TObjectManager::ReplicateObjectCreationToSecondaryMasters(TObject* object, const TCellTagList& cellTags)
{
    Impl_->ReplicateObjectCreationToSecondaryMasters(object, cellTags);
}

void TObjectManager::ReplicateObjectAttributesToSecondaryMaster(TObject* object, TCellTag cellTag)
{
    Impl_->ReplicateObjectAttributesToSecondaryMaster(object, cellTag);
}

const NProfiling::TProfiler& TObjectManager::GetProfiler()
{
    return Impl_->GetProfiler();
}

NProfiling::TMonotonicCounter* TObjectManager::GetMethodCumulativeExecuteTimeCounter(EObjectType type, const TString& method)
{
    return Impl_->GetMethodCumulativeExecuteTimeCounter(type, method);
}

TEpoch TObjectManager::GetCurrentEpoch()
{
    return Impl_->GetCurrentEpoch();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

