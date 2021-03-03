#include "object_manager.h"
#include "private.h"
#include "config.h"
#include "garbage_collector.h"
#include "master.h"
#include "master_type_handler.h"
#include "mutation_idempotizer.h"
#include "schema.h"
#include "type_handler.h"
#include "path_resolver.h"
#include "request_profiling_manager.h"
#include "helpers.h"

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/lib/transaction_server/helpers.h>

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
#include <yt/server/master/cypress_server/resolve_cache.h>

#include <yt/server/lib/election/election_manager.h>

#include <yt/server/lib/hive/hive_manager.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>
#include <yt/server/master/security_server/account.h>

#include <yt/server/master/transaction_server/boomerang_tracker.h>
#include <yt/server/master/transaction_server/transaction.h>
#include <yt/server/master/transaction_server/transaction_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/library/erasure/public.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/authentication_identity.h>

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

namespace {

TPathResolver::TResolveResult ResolvePath(
    NCellMaster::TBootstrap* bootstrap,
    const TYPath& path,
    const IServiceContextPtr& context)
{
    TPathResolver resolver(
        bootstrap,
        context->GetService(),
        context->GetMethod(),
        path,
        GetTransactionId(context));
    auto result = resolver.Resolve();
    if (result.CanCacheResolve) {
        auto populateResult = resolver.Resolve(TPathResolverOptions{
            .PopulateResolveCache = true
        });
        YT_ASSERT(std::holds_alternative<TPathResolver::TRemoteObjectPayload>(populateResult.Payload));
        auto& payload = std::get<TPathResolver::TRemoteObjectPayload>(populateResult.Payload);
        YT_LOG_DEBUG("Resolve cache populated (Path: %v, RemoteObjectId: %v, UnresolvedPathSuffix: %v)",
            path,
            payload.ObjectId,
            populateResult.UnresolvedPathSuffix);
    }
    return result;
}

} // namespace

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

    std::optional<TObject*> FindObjectByAttributes(
        EObjectType type,
        const IAttributeDictionary* attributes);

    IYPathServicePtr CreateRemoteProxy(TObjectId id);
    IYPathServicePtr CreateRemoteProxy(TCellTag cellTag);

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
        const IServiceContextPtr& context,
        const TAuthenticationIdentity& identity);
    std::unique_ptr<TMutation> CreateDestroyObjectsMutation(
        const NProto::TReqDestroyObjects& request);

    TFuture<void> GCCollect();

    TObject* CreateObject(
        TObjectId hintId,
        EObjectType type,
        IAttributeDictionary* attributes);

    bool IsObjectLifeStageValid(const TObject* object) const;
    void ValidateObjectLifeStage(const TObject* object) const;

    TObject* ResolvePathToObject(
        const TYPath& path,
        TTransaction* transaction,
        const TResolvePathOptions& options);

    void ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites);

    TFuture<TSharedRefArray> ForwardObjectRequest(
        TSharedRefArray requestMessage,
        TCellTag cellTag,
        EPeerKind peerKind);

    void ReplicateObjectCreationToSecondaryMaster(
        TObject* object,
        TCellTag cellTag);

    void ReplicateObjectCreationToSecondaryMasters(
        TObject* object,
        const TCellTagList& cellTags);

    void ReplicateObjectAttributesToSecondaryMaster(
        TObject* object,
        TCellTag cellTag);

    NProfiling::TTimeCounter* GetMethodCumulativeExecuteTimeCounter(EObjectType type, const TString& method);

    TEpoch GetCurrentEpoch();

private:
    friend class TObjectProxyBase;

    class TRootService;
    using TRootServicePtr = TIntrusivePtr<TRootService>;
    class TRemoteProxy;

    NProfiling::TBufferedProducerPtr BufferedProducer_ = New<NProfiling::TBufferedProducer>();

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
        NProfiling::TTimeCounter CumulativeExecuteTimeCounter;
    };

    THashMap<std::pair<EObjectType, TString>, std::unique_ptr<TMethodEntry>> MethodToEntry_;

    TRootServicePtr RootService_;

    TObjectId MasterObjectId_;
    std::unique_ptr<TMasterObject> MasterObject_;

    IObjectProxyPtr MasterProxy_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    TGarbageCollectorPtr GarbageCollector_;

    TMutationIdempotizerPtr MutationIdempotizer_;

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

    static TString MakeCodicilData(const TAuthenticationIdentity& identity);
    void HydraExecuteLeader(
        const TAuthenticationIdentity& identity,
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
    void HydraConfirmRemovalAwaitingCellsSyncObjects(NProto::TReqConfirmRemovalAwaitingCellsSyncObjects* request) noexcept;
    void HydraRemoveExpiredRecentlyAppliedMutationIds(NProto::TReqRemoveExpiredRecentlyAppliedMutationIds* request);

    void DoRemoveObject(TObject* object);
    void CheckRemovingObjectRefCounter(TObject* object);
    void CheckObjectLifeStageVoteCount(TObject* object);
    void ConfirmObjectLifeStageToPrimaryMaster(TObject* object);
    void AdvanceObjectLifeStageAtSecondaryMasters(TObject* object);

    void OnProfiling();

    IAttributeDictionaryPtr GetReplicatedAttributes(
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
        , ForwardedCellTag_(CellTagFromId(ObjectId_))
    { }

    TRemoteProxy(TBootstrap* bootstrap, TCellTag forwardedCellTag)
        : Bootstrap_(bootstrap)
        , ForwardedCellTag_(forwardedCellTag)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& context) override
    {
        return TResolveResultHere{path};
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        auto* mutationContext = TryGetCurrentMutationContext();
        if (mutationContext) {
            mutationContext->SetResponseKeeperSuppressed(true);
        }
        const auto& hydraManager  = Bootstrap_->GetHydraFacade()->GetHydraManager();
        bool isMutating = IsRequestMutating(context->RequestHeader());
        if (isMutating && hydraManager->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
        auto mutationId = mutationContext ? mutationContext->Request().MutationId : NullMutationId;

        auto requestMessage = context->GetRequestMessage();
        auto forwardedRequestHeader = context->RequestHeader();
        auto* forwardedYPathExt = forwardedRequestHeader.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        const auto& requestPath = GetOriginalRequestTargetYPath(context->RequestHeader());
        auto targetPathRewrite = ObjectId_
            ? MakeYPathRewrite(requestPath, ObjectId_, forwardedYPathExt->target_path())
            : MakeYPathRewrite(requestPath, requestPath);
        forwardedYPathExt->set_target_path(targetPathRewrite.Rewritten);

        SmallVector<TYPathRewrite, TypicalAdditionalPathCount> additionalPathRewrites;
        for (int index = 0; index < forwardedYPathExt->additional_paths_size(); ++index) {
            const auto& additionalPath = forwardedYPathExt->additional_paths(index);
            auto additionalResolveResult = ResolvePath(Bootstrap_, additionalPath, context);
            const auto* additionalPayload = std::get_if<TPathResolver::TRemoteObjectPayload>(&additionalResolveResult.Payload);
            if (!additionalPayload || CellTagFromId(additionalPayload->ObjectId) != ForwardedCellTag_) {
                TError error(
                    NObjectClient::EErrorCode::CrossCellAdditionalPath,
                    "Request is cross-cell since it involves target path %v and additional path %v",
                    forwardedYPathExt->original_target_path(),
                    additionalPath);

                if (mutationId) {
                    // Make sure to end the request because we may be
                    // committing a boomerang mutation right now, and
                    // replies to those are passed via the response keeper.
                    responseKeeper->EndRequest(mutationId, NRpc::CreateErrorResponseMessage(error), false);
                }

                THROW_ERROR(error);
            }
            auto additionalPathRewrite = MakeYPathRewrite(
                additionalPath,
                additionalPayload->ObjectId,
                additionalResolveResult.UnresolvedPathSuffix);
            forwardedYPathExt->set_additional_paths(index, additionalPathRewrite.Rewritten);
            additionalPathRewrites.push_back(std::move(additionalPathRewrite));
        }

        SmallVector<TYPathRewrite, 4> prerequisiteRevisionPathRewrites;
        if (forwardedRequestHeader.HasExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext)) {
            auto* prerequisitesExt = forwardedRequestHeader.MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
            for (int index = 0; index < prerequisitesExt->revisions_size(); ++index) {
                auto* prerequisite = prerequisitesExt->mutable_revisions(index);
                const auto& prerequisitePath = prerequisite->path();
                auto prerequisiteResolveResult = ResolvePath(Bootstrap_, prerequisitePath, context);
                const auto* prerequisitePayload = std::get_if<TPathResolver::TRemoteObjectPayload>(&prerequisiteResolveResult.Payload);
                if (!prerequisitePayload || CellTagFromId(prerequisitePayload->ObjectId) != ForwardedCellTag_) {
                    TError error(
                        NObjectClient::EErrorCode::CrossCellRevisionPrerequisitePath,
                        "Request is cross-cell since it involves target path %v and prerequisite reivision path %v",
                        forwardedYPathExt->original_target_path(),
                        prerequisitePath);

                    if (mutationId) {
                        // Make sure to end the request because we may be
                        // committing a boomerang mutation right now, and
                        // replies to those are passed via the response keeper.
                        responseKeeper->EndRequest(mutationId, NRpc::CreateErrorResponseMessage(error), false);
                    }

                    THROW_ERROR(error);
                }

                auto prerequisitePathRewrite = MakeYPathRewrite(
                    prerequisitePath,
                    prerequisitePayload->ObjectId,
                    prerequisiteResolveResult.UnresolvedPathSuffix);
                prerequisite->set_path(prerequisitePathRewrite.Rewritten);
                prerequisiteRevisionPathRewrites.push_back(std::move(prerequisitePathRewrite));
            }
        }

        if (auto mutationId = GetMutationId(forwardedRequestHeader)) {
            SetMutationId(&forwardedRequestHeader, GenerateNextForwardedMutationId(mutationId), forwardedRequestHeader.retry());
        }

        auto forwardedMessage = SetRequestHeader(requestMessage, forwardedRequestHeader);

        auto peerKind = isMutating ? EPeerKind::Leader : EPeerKind::Follower;

        const auto& connection = Bootstrap_->GetClusterConnection();
        auto forwardedCellId = connection->GetMasterCellId(ForwardedCellTag_);

        const auto& cellDirectory = Bootstrap_->GetCellDirectory();
        auto channel = cellDirectory->GetChannelOrThrow(forwardedCellId, peerKind);

        TObjectServiceProxy proxy(std::move(channel));
        auto batchReq = proxy.ExecuteBatchNoBackoffRetries();
        batchReq->SetOriginalRequestId(context->GetRequestId());
        batchReq->SetTimeout(ComputeForwardingTimeout(context, Bootstrap_->GetConfig()->ObjectService));
        SetAuthenticationIdentity(batchReq, context->GetAuthenticationIdentity());
        batchReq->AddRequestMessage(std::move(forwardedMessage));

        auto forwardedRequestId = batchReq->GetRequestId();

        const auto& requestProfilingManager = Bootstrap_->GetRequestProfilingManager();
        auto counters = requestProfilingManager->GetCounters(context->GetAuthenticationIdentity().UserTag, context->GetMethod());
        counters->AutomatonForwardingRequestCounter.Increment();

        YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v.%v, "
            "TargetPath: %v, %v%v%v, Mutating: %v, CellTag: %v, PeerKind: %v)",
            context->GetRequestId(),
            forwardedRequestId,
            context->GetService(),
            context->GetMethod(),
            targetPathRewrite,
            MakeFormatterWrapper([&] (auto* builder) {
               if (!additionalPathRewrites.empty()) {
                   builder->AppendFormat("AdditionalPaths: %v, ", additionalPathRewrites);
               }
            }),
            MakeFormatterWrapper([&] (auto* builder) {
               if (!additionalPathRewrites.empty()) {
                   builder->AppendFormat("PrerequisiteRevisionPaths: %v, ", prerequisiteRevisionPathRewrites);
               }
            }),
            context->GetAuthenticationIdentity(),
            isMutating,
            ForwardedCellTag_,
            peerKind);

        batchReq->Invoke().Subscribe(
            BIND([
                context,
                forwardedRequestId,
                bootstrap = Bootstrap_,
                mutationId
            ] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                const auto& responseKeeper = bootstrap->GetHydraFacade()->GetResponseKeeper();

                if (!batchRspOrError.IsOK()) {
                    YT_LOG_DEBUG(batchRspOrError, "Forwarded request failed (RequestId: %v -> %v)",
                        context->GetRequestId(),
                        forwardedRequestId);
                    auto error = TError(NObjectClient::EErrorCode::ForwardedRequestFailed, "Forwarded request failed")
                        << batchRspOrError;
                    context->Reply(error);
                    if (mutationId) {
                        responseKeeper->EndRequest(mutationId, error, false);
                    }
                    return;
                }

                YT_LOG_DEBUG("Forwarded request succeeded (RequestId: %v -> %v)",
                    context->GetRequestId(),
                    forwardedRequestId);

                const auto& batchRsp = batchRspOrError.Value();
                const auto& responseMessage = batchRsp->GetResponseMessage(0);

                context->Reply(responseMessage);
                if (mutationId) {
                    responseKeeper->EndRequest(mutationId, responseMessage, false);
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
    const TCellTag ForwardedCellTag_;
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
        if (IsRequestMutating(context->RequestHeader()) && !HasMutationContext()) {
            // Nested call or recovery.
            return TResolveResultHere{path};
        } else {
            return DoResolveThere(path, context);
        }
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto mutation = objectManager->CreateExecuteMutation(context, context->GetAuthenticationIdentity());
        mutation->SetAllowLeaderForwarding(true);
        mutation->SetCurrentTraceContext();
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


    TResolveResult DoResolveThere(const TYPath& targetPath, const IServiceContextPtr& context)
    {
        auto resolvePath = ResolvePath(Bootstrap_, targetPath, context);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto proxy = Visit(resolvePath.Payload,
            [&] (const TPathResolver::TLocalObjectPayload& targetPayload) -> IYPathServicePtr {
                return objectManager->GetProxy(targetPayload.Object, targetPayload.Transaction);
            },
            [&] (const TPathResolver::TRemoteObjectPayload& payload) -> IYPathServicePtr  {
                return objectManager->CreateRemoteProxy(payload.ObjectId);
            },
            [&] (const TPathResolver::TMissingObjectPayload& payload) -> IYPathServicePtr {
                return TNonexistingService::Get();
            });

        return TResolveResultThere{
            std::move(proxy),
            std::move(resolvePath.UnresolvedPathSuffix)
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TImpl::TImpl(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ObjectManager)
    , RootService_(New<TRootService>(Bootstrap_))
    , GarbageCollector_(New<TGarbageCollector>(Bootstrap_))
    , MutationIdempotizer_(New<TMutationIdempotizer>(Bootstrap_))
{
    ObjectServerProfiler
        .WithDefaultDisabled()
        .AddProducer("", BufferedProducer_);

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
    RegisterMethod(BIND(&TImpl::HydraConfirmRemovalAwaitingCellsSyncObjects, Unretained(this)));
    RegisterMethod(BIND(&TImpl::HydraRemoveExpiredRecentlyAppliedMutationIds, Unretained(this)));

    auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
    MasterObjectId_ = MakeWellKnownId(EObjectType::Master, primaryCellTag);
}

void TObjectManager::TImpl::Initialize()
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster()) {
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

    const auto& entry = GetOrCrash(TypeToEntry_, type);
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

        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        auto schemaObjectId = MakeSchemaObjectId(type, primaryCellTag);

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

    ++CreatedObjects_;

    if (hintId) {
        return hintId;
    }

    auto* mutationContext = GetCurrentMutationContext();
    auto version = mutationContext->GetVersion();
    auto hash = mutationContext->RandomGenerator().Generate<ui32>();

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto cellTag = multicellManager->GetCellTag();

    // NB: The highest 16 bits of hash are used for externalizing cell tag in
    // externalized transaction ids.
    if (type == EObjectType::Transaction || type == EObjectType::NestedTransaction) {
        hash &= 0xffff;
    }

    auto result = MakeRegularId(type, cellTag, version, hash);
    mutationContext->CombineStateHash(result);
    return result;
}

int TObjectManager::TImpl::RefObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!object->IsDestroyed());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object referenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectEphemeralRefCounter(object),
        GetObjectWeakRefCounter(object));

    if (object->GetLifeStage() >= EObjectLifeStage::RemovalPreCommitted) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object referenced after its removal has been pre-committed (ObjectId: %v, LifeStage: %v)",
            object->GetId(),
            object->GetLifeStage());
    }

    if (refCounter == 1) {
        GarbageCollector_->UnregisterZombie(object);
    }

    return refCounter;
}

int TObjectManager::TImpl::UnrefObject(TObject* object, int count)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(object->IsAlive());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject(count);
    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object unreferenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectEphemeralRefCounter(object),
        GetObjectWeakRefCounter(object));

    if (refCounter == 0) {
        const auto& handler = GetHandler(object);
        handler->ZombifyObject(object);

        GarbageCollector_->RegisterZombie(object);

        auto flags = handler->GetFlags();
        if (object->IsNative() &&
            Any(flags & ETypeFlags::ReplicateDestroy) &&
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
    MutationIdempotizer_->Save(context);
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
    // COMPAT(shakurov)
    if (context.GetVersion() >= EMasterReign::ShardedTransactions) {
        MutationIdempotizer_->Load(context);
    }
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
    MutationIdempotizer_->Clear();
}

void TObjectManager::TImpl::InitSchemas()
{
    for (auto& [_, entry] : TypeToEntry_) {
        entry.SchemaObject = nullptr;
        entry.SchemaProxy.Reset();
    }

    auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
    for (auto type : RegisteredTypes_) {
        if (!HasSchema(type)) {
            continue;
        }

        auto id = MakeSchemaObjectId(type, primaryCellTag);
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
    TMasterAutomatonPart::OnRecoveryStarted();

    BufferedProducer_->SetEnabled(false);

    ++CurrentEpoch_;
    GarbageCollector_->Reset();
}

void TObjectManager::TImpl::OnRecoveryComplete()
{
    TMasterAutomatonPart::OnRecoveryComplete();

    BufferedProducer_->SetEnabled(true);
}

void TObjectManager::TImpl::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnLeaderActive();

    GarbageCollector_->Start();
    MutationIdempotizer_->Start();
}

void TObjectManager::TImpl::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnStopLeading();

    GarbageCollector_->Stop();
    MutationIdempotizer_->Stop();
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

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster() &&
        Any(handler->GetFlags() & ETypeFlags::TwoPhaseRemoval))
    {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Two-phase object removal started (ObjectId: %v, RefCounter: %v)",
            object->GetId(),
            object->GetObjectRefCounter());

        object->SetLifeStage(EObjectLifeStage::RemovalStarted);
        object->ResetLifeStageVoteCount();

        NProto::TReqRemoveForeignObject request;
        ToProto(request.mutable_object_id(), object->GetId());
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

IYPathServicePtr TObjectManager::TImpl::CreateRemoteProxy(TCellTag cellTag)
{
    return New<TRemoteProxy>(Bootstrap_, cellTag);
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
    const auto& handler = FindHandler(object->GetType());
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
    for (const auto& [key, value] : branchedAttributes->Attributes()) {
        if (!value && originatingObject->IsTrunk()) {
            originatingAttributes->Remove(key);
        } else {
            originatingAttributes->Set(key, value);
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

    auto pairs = attributes.ListPairs();
    if (pairs.empty()) {
        return;
    }

    auto proxy = GetProxy(object, nullptr);
    std::sort(pairs.begin(), pairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    for (const auto& [key, value] : pairs) {
        proxy->MutableAttributes()->Set(key, value);
    }
}

std::unique_ptr<TMutation> TObjectManager::TImpl::CreateExecuteMutation(
    const IServiceContextPtr& context,
    const TAuthenticationIdentity& identity)
{
    NProto::TReqExecute request;
    WriteAuthenticationIdentityToProto(&request, identity);

    auto requestMessage = context->GetRequestMessage();
    for (const auto& part : requestMessage) {
        request.add_request_parts(part.Begin(), part.Size());
    }

    auto mutation = CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request);
    mutation->SetHandler(BIND_DONT_CAPTURE_TRACE_CONTEXT(
        &TImpl::HydraExecuteLeader,
        MakeStrong(this),
        identity,
        MakeCodicilData(identity),
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
        THROW_ERROR_EXCEPTION("Unknown object type %Qlv",
            type);
    }

    auto flags = handler->GetFlags();
    if (None(flags & ETypeFlags::Creatable)) {
        THROW_ERROR_EXCEPTION("Objects of type %Qlv cannot be created explicitly",
            type);
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    bool replicate =
        multicellManager->IsPrimaryMaster() &&
        Any(flags & ETypeFlags::ReplicateCreate);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto* schema = FindSchema(type);
    if (schema) {
        securityManager->ValidatePermission(schema, user, EPermission::Create);
    }

    IAttributeDictionaryPtr attributeHolder;
    if (auto* attributeSet = schema->GetAttributes()) {
        attributeHolder = CreateEphemeralAttributes();
        for (const auto& [key, value] : attributeSet->Attributes()) {
            attributeHolder->SetYson(key, value);
        }
        if (attributes) {
            auto attributeMap = PatchNode(attributeHolder->ToMap(), attributes->ToMap());
            attributeHolder = IAttributeDictionary::FromMap(attributeMap->AsMap());
        }
        attributes = attributeHolder.Get();
    } else if (!attributes) {
        attributeHolder = CreateEphemeralAttributes();
        attributes = attributeHolder.Get();
    }

    // ITypeHandler::CreateObject may modify the attributes.
    IAttributeDictionaryPtr replicatedAttributes;
    if (replicate) {
        replicatedAttributes = attributes->Clone();
    }

    auto* object = handler->CreateObject(hintId, attributes);

    if (multicellManager->IsMulticell() && Any(flags & ETypeFlags::TwoPhaseCreation)) {
        object->SetLifeStage(EObjectLifeStage::CreationStarted);
        object->ResetLifeStageVoteCount();
        YT_VERIFY(object->IncrementLifeStageVoteCount() == 1);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Two-phase object creation started (ObjectId: %v)",
            object->GetId());
    } else {
        object->SetLifeStage(EObjectLifeStage::CreationCommitted);
    }

    YT_VERIFY(object->GetObjectRefCounter() == 1);

    if (object->GetNativeCellTag() != multicellManager->GetCellTag()) {
        object->SetForeign();
    }

    if (auto lifeStage = attributes->Find<EObjectLifeStage>("life_stage")) {
        attributes->Remove("life_stage");
        object->SetLifeStage(*lifeStage);
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

    switch (object->GetLifeStage()) {
        case EObjectLifeStage::RemovalPreCommitted:
            object->SetLifeStage(EObjectLifeStage::RemovalStarted);
            /* fallthrough */

        case EObjectLifeStage::RemovalStarted:
            CheckRemovingObjectRefCounter(object);
            break;

        case EObjectLifeStage::RemovalAwaitingCellsSync:
            object->SetLifeStage(EObjectLifeStage::RemovalPreCommitted);
            break;

        default:
            ConfirmObjectLifeStageToPrimaryMaster(object);
    }

    return object;
}

bool TObjectManager::TImpl::IsObjectLifeStageValid(const TObject* object) const
{
    YT_VERIFY(IsObjectAlive(object));

    if (NHiveServer::IsHiveMutation() && !NTransactionServer::IsBoomerangMutation()) {
        return true;
    }

    if (object->GetLifeStage() == EObjectLifeStage::CreationCommitted) {
        return true;
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();

    if (multicellManager->IsSecondaryMaster() &&
        object->GetLifeStage() == EObjectLifeStage::CreationPreCommitted)
    {
        return true;
    }

    return false;
}

void TObjectManager::TImpl::ValidateObjectLifeStage(const TObject* object) const
{
    if (!IsObjectLifeStageValid(object)) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::InvalidObjectLifeStage,
            "%v cannot be used since it is in %Qlv life stage",
            object->GetCapitalizedObjectName(),
            object->GetLifeStage());
    }
}

std::optional<TObject*> TObjectManager::TImpl::FindObjectByAttributes(
    EObjectType type,
    const IAttributeDictionary* attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = FindHandler(type);
    if (!handler) {
        THROW_ERROR_EXCEPTION("Unknown object type %Qlv",
            type);
    }

    return handler->FindObjectByAttributes(attributes);
}

void TObjectManager::TImpl::ConfirmObjectLifeStageToPrimaryMaster(TObject* object)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (!multicellManager->IsSecondaryMaster()) {
        return;
    }

    auto lifeStage = object->GetLifeStage();
    if (lifeStage == EObjectLifeStage::CreationCommitted ||
        lifeStage == EObjectLifeStage::RemovalCommitted) {
        return;
    }

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Confirming object life stage to primary master (ObjectId: %v, LifeStage: %v)",
        object->GetId(),
        lifeStage);

    NProto::TReqConfirmObjectLifeStage request;
    ToProto(request.mutable_object_id(), object->GetId());
    request.set_cell_tag(multicellManager->GetCellTag());
    multicellManager->PostToPrimaryMaster(request);
}

void TObjectManager::TImpl::AdvanceObjectLifeStageAtSecondaryMasters(NYT::NObjectServer::TObject* object)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Advancing object life stage at secondary masters (ObjectId: %v, LifeStage: %v)",
        object->GetId(),
        object->GetLifeStage());

    NProto::TReqAdvanceObjectLifeStage advanceRequest;
    ToProto(advanceRequest.mutable_object_id(), object->GetId());
    advanceRequest.set_new_life_stage(static_cast<int>(object->GetLifeStage()));
    multicellManager->PostToSecondaryMasters(advanceRequest);
}

TObject* TObjectManager::TImpl::ResolvePathToObject(const TYPath& path, TTransaction* transaction, const TResolvePathOptions& options)
{
    static const TString NullService;
    static const TString NullMethod;
    TPathResolver resolver(
        Bootstrap_,
        NullService,
        NullMethod,
        path,
        transaction);

    auto result = resolver.Resolve(TPathResolverOptions{
        .EnablePartialResolve = options.EnablePartialResolve,
        .FollowPortals = options.FollowPortals
    });
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
            ThrowPrerequisiteCheckFailedNoSuchTransaction(transactionId);
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
        const auto& path = prerequisite.path();
        auto revision = prerequisite.revision();

        TCypressNode* trunkNode;
        try {
            trunkNode = cypressManager->ResolvePathToTrunkNode(path);
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
                "Prerequisite check failed: node %v revision mismatch: expected %llx, found %llx",
                path,
                revision,
                trunkNode->GetRevision());
        }
    }
}

TFuture<TSharedRefArray> TObjectManager::TImpl::ForwardObjectRequest(
    TSharedRefArray requestMessage,
    TCellTag cellTag,
    EPeerKind peerKind)
{
    VERIFY_THREAD_AFFINITY_ANY();

    NRpc::NProto::TRequestHeader header;
    YT_VERIFY(ParseRequestHeader(requestMessage, &header));

    auto requestId = FromProto<TRequestId>(header.request_id());
    const auto& ypathExt = header.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    if (auto mutationId = GetMutationId(header)) {
        SetMutationId(&header, GenerateNextForwardedMutationId(mutationId), header.retry());
    }

    auto timeout = ComputeForwardingTimeout(
        FromProto<TDuration>(header.timeout()),
        Bootstrap_->GetConfig()->ObjectService);

    const auto& connection = Bootstrap_->GetClusterConnection();
    auto cellId = connection->GetMasterCellId(cellTag);

    const auto& cellDirectory = Bootstrap_->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(cellId, peerKind);

    auto identity = ParseAuthenticationIdentityFromProto(header);

    TObjectServiceProxy proxy(std::move(channel));
    auto batchReq = proxy.ExecuteBatchNoBackoffRetries();
    batchReq->SetOriginalRequestId(requestId);
    batchReq->SetTimeout(timeout);
    batchReq->AddRequestMessage(std::move(requestMessage));
    SetAuthenticationIdentity(batchReq, identity);

    YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v.%v, Path: %v, %v, Mutating: %v, "
        "CellTag: %v, PeerKind: %v)",
        requestId,
        batchReq->GetRequestId(),
        header.service(),
        header.method(),
        ypathExt.target_path(),
        identity,
        ypathExt.mutating(),
        cellTag,
        peerKind);

    return batchReq->Invoke().Apply(BIND([=] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
        if (!batchRspOrError.IsOK()) {
            YT_LOG_DEBUG(batchRspOrError, "Forwarded request failed (RequestId: %v -> %v)",
                requestId,
                batchReq->GetRequestId());
            auto error = TError(NObjectClient::EErrorCode::ForwardedRequestFailed, "Forwarded request failed")
                << batchRspOrError;
            return CreateErrorResponseMessage(requestId, batchRspOrError);
        }

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
    req->set_value(ConvertToYsonString(GetReplicatedAttributes(object, false)->ToMap()).ToString());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(req, cellTag);
}

TString TObjectManager::TImpl::MakeCodicilData(const TAuthenticationIdentity& identity)
{
    return ToString(identity);
}

void TObjectManager::TImpl::HydraExecuteLeader(
    const TAuthenticationIdentity& identity,
    const TString& codicilData,
    const IServiceContextPtr& rpcContext,
    TMutationContext* mutationContext)
{
    TWallTimer timer;

    TCodicilGuard codicilGuard(codicilData);

    auto mutationId = rpcContext->GetMutationId();
    if (mutationId && MutationIdempotizer_->IsMutationApplied(mutationId)) {
        // Usually, the response keeper protects us from duplicate mutations,
        // since no request can be executed while another request is in between
        // its "begin" and "end".
        //
        // However, a boomerang may return in precisely that (unfortunate)
        // interval. If the boomerang's "begin" has been lost due to a recent
        // leader change, we get here.

        auto errorResponse = TError("Mutation is already applied")
            << TErrorAttribute("mutation_id", mutationId);

        rpcContext->Reply(errorResponse);

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& responseKeeper = hydraFacade->GetResponseKeeper();
        responseKeeper->EndRequest(mutationId, NRpc::CreateErrorResponseMessage(errorResponse));

        YT_LOG_WARNING("Duplicate mutation application skipped (MutationId: %v)",
            mutationId);

        return;
    }

    std::optional<NTracing::TChildTraceContextGuard> traceContextGuard;
    if (auto* traceContext = NTracing::GetCurrentTraceContext()) {
        traceContextGuard.emplace(
            traceContext,
            ConcatToString(TStringBuf("YPathWrite:"), rpcContext->GetService(), TStringBuf("."), rpcContext->GetMethod()));
    }

    const auto& securityManager = Bootstrap_->GetSecurityManager();

    TUser* user = nullptr;
    try {
        TAuthenticatedUserGuard userGuard(securityManager, identity);
        user = userGuard.GetUser();
        ExecuteVerb(RootService_, rpcContext);
    } catch (const std::exception& ex) {
        rpcContext->Reply(ex);
    }

    if (!IsRecovery()) {
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, timer.GetElapsedTime()});
    }

    if (mutationId && !mutationContext->GetResponseKeeperSuppressed()) {
        MutationIdempotizer_->SetMutationApplied(mutationId);

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
        requestMessageBuilder.Add(TSharedRef::FromString(part));
    }

    auto requestMessage = requestMessageBuilder.Finish();

    auto context = CreateYPathContext(
        std::move(requestMessage),
        IsMutationLoggingEnabled() ? ObjectServerLogger : NLogging::TLogger(),
        NLogging::ELogLevel::Debug);

    auto identity = ParseAuthenticationIdentityFromProto(*request);

    auto codicilData = MakeCodicilData(identity);

    HydraExecuteLeader(identity, codicilData, context, GetCurrentMutationContext());
}

void TObjectManager::TImpl::HydraDestroyObjects(NProto::TReqDestroyObjects* request)
{
    // NB: Ordered map is a must to make the behavior deterministic.
    std::map<TCellTag, NProto::TReqUnrefExportedObjects> crossCellRequestMap;
    auto getCrossCellRequest = [&] (TObjectId id) -> NProto::TReqUnrefExportedObjects& {
        return crossCellRequestMap[CellTagFromId(id)];
    };

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    for (const auto& protoId : request->object_ids()) {
        auto id = FromProto<TObjectId>(protoId);
        auto type = TypeFromId(id);

        const auto& handler = GetHandler(type);
        auto* object = handler->FindObject(id);

        if (!object || object->GetObjectRefCounter() > 0) {
            continue;
        }

        if (object->IsForeign() && object->GetImportRefCounter() > 0) {
            auto& crossCellRequest = getCrossCellRequest(id);
            crossCellRequest.set_cell_tag(multicellManager->GetCellTag());
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

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object destroyed (Type: %v, Id: %v)",
            type,
            id);
    }

    for (const auto& [cellTag, perCellRequest] : crossCellRequestMap) {
        multicellManager->PostToMaster(perCellRequest, cellTag);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Requesting to unreference imported objects (CellTag: %v, Count: %v)",
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
        : nullptr;

    CreateObject(
        objectId,
        type,
        attributes.Get());

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Foreign object created (Id: %v, Type: %v)",
        objectId,
        type);
}

void TObjectManager::TImpl::HydraRemoveForeignObject(NProto::TReqRemoveForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());

    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Attempt to remove a non-existing foreign object (ObjectId: %v)",
            objectId);
        return;
    }

    if (object->GetLifeStage() != EObjectLifeStage::CreationCommitted) {
        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
            "Requested to remove a foreign object with inappropriate life stage (ObjectId: %v, LifeStage: %v)",
            objectId,
            object->GetLifeStage());
        return;
    }

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Removing foreign object (ObjectId: %v, RefCounter: %v)",
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

        auto* object = FindObject(objectId);

        if (!IsObjectAlive(object)) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                "Requested to unexport non-existing object (ObjectId: %v, ImportRefCounter: %v, CellTag: %v)",
                objectId,
                importRefCounter,
                cellTag);
            continue;
        }

        UnrefObject(object, importRefCounter);

        const auto& handler = GetHandler(object);
        handler->UnexportObject(object, cellTag, importRefCounter);
    }

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Exported objects unreferenced (CellTag: %v, Count: %v)",
        cellTag,
        request->entries_size());
}

void TObjectManager::TImpl::HydraConfirmObjectLifeStage(NProto::TReqConfirmObjectLifeStage* confirmRequest) noexcept
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    auto objectId = FromProto<TObjectId>(confirmRequest->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "A non-existing object creation confirmed by secondary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto voteCount = object->IncrementLifeStageVoteCount();
    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
        "Object life stage confirmed by secondary cell (ObjectId: %v, CellTag: %v, VoteCount: %v)",
        objectId,
        confirmRequest->cell_tag(),
        voteCount);

    CheckObjectLifeStageVoteCount(object);
}

void TObjectManager::TImpl::HydraAdvanceObjectLifeStage(NProto::TReqAdvanceObjectLifeStage* request) noexcept
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsSecondaryMaster());

    auto objectId = FromProto<TObjectId>(request->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Life stage advancement for a non-existing object requested by the primary cell (ObjectId: %v)",
            objectId);
        return;
    }

    if (object->IsNative()) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Life stage advancement for a non-foreign object requested by primary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto oldLifeStage = object->GetLifeStage();
    auto newLifeStage = CheckedEnumCast<EObjectLifeStage>(request->new_life_stage());
    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
        "Object life stage advanced by primary cell (ObjectId: %v, LifeStage: %v -> %v)",
        objectId,
        oldLifeStage,
        newLifeStage);

    ConfirmObjectLifeStageToPrimaryMaster(object);

    if (newLifeStage == EObjectLifeStage::RemovalCommitted) {
        DoRemoveObject(object);
    }
}

void TObjectManager::TImpl::HydraConfirmRemovalAwaitingCellsSyncObjects(NProto::TReqConfirmRemovalAwaitingCellsSyncObjects* request) noexcept
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    auto objectIds = FromProto<std::vector<TObjectId>>(request->object_ids());
    for (auto objectId : objectIds) {
        auto* object = FindObject(objectId);
        if (!object) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                "Cells sync confirmed for a non-existing object (ObjectId: %v)",
                objectId);
            continue;
        }

        auto oldLifeStage = object->GetLifeStage();
        if (oldLifeStage != EObjectLifeStage::RemovalAwaitingCellsSync) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                "Cells sync confirmed for an object with invalid life stage (ObjectId: %v, LifeStage: %v)",
                objectId,
                oldLifeStage);
            continue;
        }

        auto newLifeStage = EObjectLifeStage::RemovalCommitted;
        object->SetLifeStage(newLifeStage);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Object life stage advanced after cells sync (ObjectId: %v, LifeStage: %v -> %v)",
            objectId,
            oldLifeStage,
            newLifeStage);

        AdvanceObjectLifeStageAtSecondaryMasters(object);
        GarbageCollector_->UnregisterRemovalAwaitingCellsSyncObject(object);
        DoRemoveObject(object);
    }
}

void TObjectManager::TImpl::HydraRemoveExpiredRecentlyAppliedMutationIds(NProto::TReqRemoveExpiredRecentlyAppliedMutationIds* /*request*/)
{
    MutationIdempotizer_->RemoveExpiredMutations();
}

void TObjectManager::TImpl::DoRemoveObject(TObject* object)
{
    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object removed (ObjectId: %v)",
        object->GetId());
    if (UnrefObject(object) != 0) {
        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
            "Non-zero reference counter after object removal (ObjectId: %v, RefCounter: %v)",
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
    YT_VERIFY(oldLifeStage == EObjectLifeStage::RemovalStarted);
    auto newLifeStage = EObjectLifeStage::RemovalPreCommitted;

    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object references released (ObjectId: %v, LifeStage: %v -> %v)",
        object->GetId(),
        oldLifeStage,
        newLifeStage);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster()) {
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
        const auto& secondaryCellTags = Bootstrap_->GetMulticellManager()->GetSecondaryCellTags();
        YT_VERIFY(voteCount <= secondaryCellTags.size() + 1);
        if (voteCount < secondaryCellTags.size() + 1) {
            break;
        }

        auto oldLifeStage = object->GetLifeStage();
        EObjectLifeStage newLifeStage;
        switch (oldLifeStage) {
            case EObjectLifeStage::CreationStarted:
                newLifeStage = EObjectLifeStage::CreationPreCommitted;
                break;
            case EObjectLifeStage::CreationPreCommitted:
                newLifeStage = EObjectLifeStage::CreationCommitted;
                break;
            case EObjectLifeStage::RemovalStarted:
                YT_ABORT();
                break;
            case EObjectLifeStage::RemovalPreCommitted:
                newLifeStage = EObjectLifeStage::RemovalAwaitingCellsSync;
                break;
            default:
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Unexpected object life stage (ObjectId: %v, LifeStage: %v)",
                    object->GetId(),
                    oldLifeStage);
                return;
        }

        object->SetLifeStage(newLifeStage);
        object->ResetLifeStageVoteCount();

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Object life stage votes collected; advancing life stage (ObjectId: %v, LifeStage: %v -> %v)",
            object->GetId(),
            oldLifeStage,
            newLifeStage);

        if (newLifeStage == EObjectLifeStage::RemovalAwaitingCellsSync) {
            GarbageCollector_->RegisterRemovalAwaitingCellsSyncObject(object);
        } else {
            AdvanceObjectLifeStageAtSecondaryMasters(object);
            if (newLifeStage == EObjectLifeStage::CreationPreCommitted) {
                object->IncrementLifeStageVoteCount();
            }
        }
    }
}

NProfiling::TTimeCounter* TObjectManager::TImpl::GetMethodCumulativeExecuteTimeCounter(
    EObjectType type,
    const TString& method)
{
    auto key = std::make_pair(type, method);
    auto it = MethodToEntry_.find(key);
    if (it == MethodToEntry_.end()) {
        auto entry = std::make_unique<TMethodEntry>();
        entry->CumulativeExecuteTimeCounter = ObjectServerProfiler
            .WithTag("type", FormatEnum(type))
            .WithTag("method", method)
            .TimeCounter("/cumulative_execute_time");
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

    TSensorBuffer buffer;
    buffer.AddGauge("/zombie_object_count", GarbageCollector_->GetZombieCount());
    buffer.AddGauge("/ephemeral_ghost_object_count", GarbageCollector_->GetEphemeralGhostCount());
    buffer.AddGauge("/weak_ghost_object_count", GarbageCollector_->GetWeakGhostCount());
    buffer.AddGauge("/locked_object_count", GarbageCollector_->GetLockedCount());
    buffer.AddCounter("/created_objects", CreatedObjects_);
    buffer.AddCounter("/destroyed_objects", DestroyedObjects_);
    buffer.AddGauge("/recently_finished_mutation_count", MutationIdempotizer_->RecentlyFinishedMutationCount());
    BufferedProducer_->Update(std::move(buffer));
}

IAttributeDictionaryPtr TObjectManager::TImpl::GetReplicatedAttributes(
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
            const auto& key = descriptor.InternedKey.Unintern();
            replicateKey(key, value);
        }
    }

    // Check custom attributes.
    const auto* customAttributes = object->GetAttributes();
    if (customAttributes) {
        for (const auto& [key, value] : object->GetAttributes()->Attributes()) {
            replicateKey(key, value);
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

TObjectManager::~TObjectManager()
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

IYPathServicePtr TObjectManager::CreateRemoteProxy(TCellTag cellTag)
{
    return Impl_->CreateRemoteProxy(cellTag);
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

std::unique_ptr<TMutation> TObjectManager::CreateExecuteMutation(const IServiceContextPtr& context, const TAuthenticationIdentity& identity)
{
    return Impl_->CreateExecuteMutation(context, identity);
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

bool TObjectManager::IsObjectLifeStageValid(const TObject* object) const
{
    return Impl_->IsObjectLifeStageValid(object);
}

void TObjectManager::ValidateObjectLifeStage(const TObject* object) const
{
    return Impl_->ValidateObjectLifeStage(object);
}

std::optional<TObject*> TObjectManager::FindObjectByAttributes(
    EObjectType type,
    const NYTree::IAttributeDictionary* attributes)
{
    return Impl_->FindObjectByAttributes(type, attributes);
}

TObject* TObjectManager::ResolvePathToObject(const TYPath& path, TTransaction* transaction, const TResolvePathOptions& options)
{
    return Impl_->ResolvePathToObject(path, transaction, options);
}

void TObjectManager::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
{
    Impl_->ValidatePrerequisites(prerequisites);
}

TFuture<TSharedRefArray> TObjectManager::ForwardObjectRequest(
    TSharedRefArray requestMessage,
    TCellTag cellTag,
    EPeerKind peerKind)
{
    return Impl_->ForwardObjectRequest(std::move(requestMessage), cellTag, peerKind);
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

NProfiling::TTimeCounter* TObjectManager::GetMethodCumulativeExecuteTimeCounter(EObjectType type, const TString& method)
{
    return Impl_->GetMethodCumulativeExecuteTimeCounter(type, method);
}

TEpoch TObjectManager::GetCurrentEpoch()
{
    return Impl_->GetCurrentEpoch();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

