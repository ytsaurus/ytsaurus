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

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/helpers.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/resolve_cache.h>

#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>
#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/boomerang_tracker.h>
#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>
#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/node_detail.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/misc/variant.h>

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
using namespace NTransactionSupervisor;
using namespace NSequoiaClient;

using TYPath = NYPath::TYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;
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

bool IsObjectLifeStageValidationSuppressed()
{
    return IsSubordinateMutation();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TObjectManager
    : public IObjectManager
    , public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TObjectManager(NCellMaster::TBootstrap* bootstrap);

    TObjectManager(
        TTestingTag,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize() override;

    void RegisterHandler(IObjectTypeHandlerPtr handler) override;
    const IObjectTypeHandlerPtr& FindHandler(EObjectType type) const override;
    const IObjectTypeHandlerPtr& GetHandler(EObjectType type) const override;
    const IObjectTypeHandlerPtr& GetHandlerOrThrow(EObjectType type) const override;
    const IObjectTypeHandlerPtr& GetHandler(const TObject* object) const override;

    const std::set<EObjectType>& GetRegisteredTypes() const override;

    TObjectId GenerateId(EObjectType type, TObjectId hintId) override;

    int RefObject(TObject* object) override;
    int UnrefObject(TObject* object, int count = 1) override;
    int EphemeralRefObject(TObject* object) override;
    void EphemeralUnrefObject(TObject* object) override;
    void EphemeralUnrefObject(TObject* object, TEpoch epoch) override;
    int WeakRefObject(TObject* object) override;
    int WeakUnrefObject(TObject* object) override;

    TObject* FindObject(TObjectId id) override;
    TObject* GetObject(TObjectId id) override;
    TObject* GetObjectOrThrow(TObjectId id) override;
    TObject* GetWeakGhostObject(TObjectId id) override;
    void RemoveObject(TObject* object) override;

    std::optional<TObject*> FindObjectByAttributes(
        EObjectType type,
        const IAttributeDictionary* attributes) override;

    IYPathServicePtr CreateRemoteProxy(TObjectId id) override;
    IYPathServicePtr CreateRemoteProxy(TCellTag cellTag) override;

    IObjectProxyPtr GetProxy(
        TObject* object,
        TTransaction* transaction = nullptr) override;

    void BranchAttributes(
        const TObject* originatingObject,
        TObject* branchedObject) override;
    void MergeAttributes(
        TObject* originatingObject,
        const TObject* branchedObject) override;
    void FillAttributes(
        TObject* object,
        const IAttributeDictionary& attributes) override;

    IYPathServicePtr GetRootService() override;

    TObject* GetMasterObject() override;
    IObjectProxyPtr GetMasterProxy() override;

    TObject* FindSchema(EObjectType type) override;
    TObject* GetSchema(EObjectType type) override;
    IObjectProxyPtr GetSchemaProxy(EObjectType type) override;

    std::unique_ptr<TMutation> CreateExecuteMutation(
        const IYPathServiceContextPtr& context,
        const TAuthenticationIdentity& identity) override;
    std::unique_ptr<TMutation> CreateDestroyObjectsMutation(
        const NProto::TReqDestroyObjects& request) override;

    TFuture<void> DestroyObjects(std::vector<TObjectId> objectIds) override;

    TFuture<void> GCCollect() override;

    TObject* CreateObject(
        TObjectId hintId,
        EObjectType type,
        IAttributeDictionary* attributes) override;

    bool IsObjectLifeStageValid(const TObject* object) const override;
    void ValidateObjectLifeStage(const TObject* object) const override;

    TObject* ResolvePathToObject(
        const TYPath& path,
        TTransaction* transaction,
        const TResolvePathOptions& options) override;

    TFuture<std::vector<TErrorOr<TVersionedObjectPath>>> ResolveObjectIdsToPaths(
        const std::vector<TVersionedObjectId>& objectIds) override;

    void ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites) override;

    TFuture<TSharedRefArray> ForwardObjectRequest(
        const TSharedRefArray& requestMessage,
        TCellTag cellTag,
        NApi::EMasterChannelKind channelKind) override;

    void ReplicateObjectCreationToSecondaryMaster(
        TObject* object,
        TCellTag cellTag) override;

    void ReplicateObjectAttributesToSecondaryMaster(
        TObject* object,
        TCellTag cellTag) override;

    NProfiling::TTimeCounter* GetMethodCumulativeExecuteTimeCounter(EObjectType type, const TString& method) override;

    const TGarbageCollectorPtr& GetGarbageCollector() const override;

private:
    friend class TObjectProxyBase;

    class TRootService;
    using TRootServicePtr = TIntrusivePtr<TRootService>;

    class TRemoteProxy;

    const NProfiling::TBufferedProducerPtr BufferedProducer_ = New<NProfiling::TBufferedProducer>();

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

    TSyncMap<std::pair<EObjectType, TString>, std::unique_ptr<TMethodEntry>> MethodToEntry_;

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

    // COMPAT(h0pless): FixTransactionACLs
    bool ResetTransactionAcls_ = false;

    // COMPAT(babenko)
    bool DropLegacyClusterNodeMap_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;

    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    void OnAfterSnapshotLoaded() override;

    void OnRecoveryStarted() override;
    void OnRecoveryComplete() override;
    void Clear() override;
    void OnLeaderActive() override;
    void OnStopLeading() override;
    void OnStartFollowing() override;
    void OnStopFollowing() override;

    void CheckInvariants() override;

    static TString MakeCodicilData(const TAuthenticationIdentity& identity);
    void HydraExecuteLeader(
        const TAuthenticationIdentity& identity,
        const TString& codicilData,
        const IYPathServiceContextPtr& rpcContext,
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
    void HydraPrepareDestroyObjects(
        TTransaction* transaction,
        NProto::TReqDestroyObjects* request,
        const TTransactionPrepareOptions& options);

    TFuture<void> DestroySequoiaObjects(NProto::TReqDestroyObjects request);

    void DoRemoveObject(TObject* object);
    void SendObjectLifeStageConfirmation(TObjectId objectId);
    void CheckRemovingObjectRefCounter(TObject* object);
    void CheckObjectLifeStageVoteCount(TObject* object);
    void ConfirmObjectLifeStageToPrimaryMaster(TObject* object);
    void AdvanceObjectLifeStageAtSecondaryMasters(TObject* object);

    void DoDestroyObjects(NProto::TReqDestroyObjects* request) noexcept;

    void OnProfiling();

    IAttributeDictionaryPtr GetReplicatedAttributes(
        TObject* object,
        bool mandatory,
        bool writableOnly);

    void DoReplicateObjectAttributesToSecondaryMaster(
        TObject* object,
        TCellTag cellTag,
        bool mandatory);

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag);

    const TDynamicObjectManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/);

    void InitSchemas();
};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRemoteProxy
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

    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    void Invoke(const IYPathServiceContextPtr& context) override
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

        const auto& requestPath = GetOriginalRequestTargetYPath(context->RequestHeader());
        context->SetRequestInfo("Method: %v.%v, Path: %v",
            context->GetService(),
            context->GetMethod(),
            requestPath);

        const auto& responseKeeper = Bootstrap_->GetHydraFacade()->GetResponseKeeper();
        auto mutationId = mutationContext ? mutationContext->Request().MutationId : NullMutationId;

        auto requestMessage = context->GetRequestMessage();
        auto forwardedRequestHeader = context->RequestHeader();
        auto* forwardedYPathExt = forwardedRequestHeader.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        auto targetPathRewrite = ObjectId_
            ? MakeYPathRewrite(requestPath, ObjectId_, forwardedYPathExt->target_path())
            : MakeYPathRewrite(requestPath, requestPath);
        forwardedYPathExt->set_target_path(targetPathRewrite.Rewritten);

        TCompactVector<TYPathRewrite, TypicalAdditionalPathCount> additionalPathRewrites;
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
                    if (auto setResponseKeeperPromise =
                        responseKeeper->EndRequest(mutationId, NRpc::CreateErrorResponseMessage(error), /*remember*/ false))
                    {
                        setResponseKeeperPromise();
                    }
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

        TCompactVector<TYPathRewrite, 4> prerequisiteRevisionPathRewrites;
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
                        "Request is cross-cell since it involves target path %v and prerequisite revision path %v",
                        forwardedYPathExt->original_target_path(),
                        prerequisitePath);

                    if (mutationId) {
                        // Make sure to end the request because we may be
                        // committing a boomerang mutation right now, and
                        // replies to those are passed via the response keeper.
                        if (auto setResponseKeeperPromise =
                            responseKeeper->EndRequest(mutationId, NRpc::CreateErrorResponseMessage(error), false))
                        {
                            setResponseKeeperPromise();
                        }
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

        auto channelKind = isMutating ? NApi::EMasterChannelKind::Leader : NApi::EMasterChannelKind::Follower;

        TObjectServiceProxy proxy(
            Bootstrap_->GetClusterConnection(),
            channelKind,
            ForwardedCellTag_,
            /*stickyGroupSizeCache*/ nullptr);
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
            "TargetPath: %v, %v%v%v, Mutating: %v, CellTag: %v, ChannelKind: %v)",
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
            channelKind);

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
                        if (auto setResponseKeeperPromise = responseKeeper->EndRequest(mutationId, error, false)) {
                            setResponseKeeperPromise();
                        }
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
                    if (auto setResponseKeeperPromise = responseKeeper->EndRequest(mutationId, responseMessage, false)) {
                        setResponseKeeperPromise();
                    }
                }
            }).Via(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService)));
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*attributeFilter*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    bool ShouldHideAttributes() override
    {
        return false;
    }

private:
    TBootstrap* const Bootstrap_;
    const TObjectId ObjectId_;
    const TCellTag ForwardedCellTag_;
};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TResolveResult Resolve(const TYPath& path, const IYPathServiceContextPtr& context) override
    {
        if (IsRequestMutating(context->RequestHeader()) && !HasHydraContext()) {
            // Nested call or recovery.
            return TResolveResultHere{path};
        } else {
            return DoResolveThere(path, context);
        }
    }

    void Invoke(const IYPathServiceContextPtr& context) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto mutation = objectManager->CreateExecuteMutation(context, context->GetAuthenticationIdentity());
        mutation->SetAllowLeaderForwarding(true);
        mutation->SetCurrentTraceContext();
        mutation->Commit()
            .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    // Reply with commit error.
                    context->Reply(resultOrError);
                    return;
                }
                const auto& result = resultOrError.Value();
                if (result.Origin != EMutationResponseOrigin::Commit) {
                    // Reply with explicit response.
                    context->Reply(result.Data);
                    return;
                }
            }));
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TAttributeFilter& /*attributeFilter*/,
        bool /*stable*/) override
    {
        YT_ABORT();
    }

    bool ShouldHideAttributes() override
    {
        YT_ABORT();
    }

private:
    TBootstrap* const Bootstrap_;


    TResolveResult DoResolveThere(const TYPath& targetPath, const IYPathServiceContextPtr& context)
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
            [&] (const TPathResolver::TMissingObjectPayload& /*payload*/) -> IYPathServicePtr {
                return TNonexistingService::Get();
            },
            [&] (const TPathResolver::TSequoiaRedirectPayload& payload) -> IYPathServicePtr {
                THROW_ERROR_EXCEPTION(NObjectClient::EErrorCode::RequestInvolvesSequoia,
                    "Request involves Sequoia shard")
                    << TErrorAttribute("rootstock_node_id", payload.RootstockNodeId)
                    << TErrorAttribute("rootstock_path", payload.RootstockPath);
            });

        return TResolveResultThere{
            std::move(proxy),
            std::move(resolvePath.UnresolvedPathSuffix)
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(TBootstrap* bootstrap)
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

    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraExecuteFollower, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraDestroyObjects, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraCreateForeignObject, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraRemoveForeignObject, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraUnrefExportedObjects, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraConfirmObjectLifeStage, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraAdvanceObjectLifeStage, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraConfirmRemovalAwaitingCellsSyncObjects, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TObjectManager::HydraRemoveExpiredRecentlyAppliedMutationIds, Unretained(this)));

    auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
    MasterObjectId_ = MakeWellKnownId(EObjectType::Master, primaryCellTag);
}

TObjectManager::TObjectManager(TTestingTag tag, TBootstrap* bootstrap)
    : TMasterAutomatonPart(tag, bootstrap)
    , RootService_(New<TRootService>(Bootstrap_))
    , GarbageCollector_(New<TGarbageCollector>(Bootstrap_))
{ }

void TObjectManager::Initialize()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TObjectManager::OnDynamicConfigChanged, MakeWeak(this)));

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    transactionManager->RegisterTransactionActionHandlers<NProto::TReqDestroyObjects>({
        .Prepare = BIND_NO_PROPAGATE(&TObjectManager::HydraPrepareDestroyObjects, Unretained(this)),
    });

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster()) {
        multicellManager->SubscribeReplicateValuesToSecondaryMaster(
            BIND_NO_PROPAGATE(&TObjectManager::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
    }

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TObjectManager::OnProfiling, MakeWeak(this)),
        TDynamicObjectManagerConfig::DefaultProfilingPeriod);
    ProfilingExecutor_->Start();
}

IYPathServicePtr TObjectManager::GetRootService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootService_;
}

TObject* TObjectManager::GetMasterObject()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterObject_.get();
}

IObjectProxyPtr TObjectManager::GetMasterProxy()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterProxy_;
}

TObject* TObjectManager::FindSchema(EObjectType type)
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

IObjectProxyPtr TObjectManager::GetSchemaProxy(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& entry = GetOrCrash(TypeToEntry_, type);
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
    YT_VERIFY(handler);

    return handler;
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandlerOrThrow(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& handler = FindHandler(type);
    if (!handler) {
        THROW_ERROR_EXCEPTION("Unknown object type %Qlv",
            type);
    }

    return handler;
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(const TObject* object) const
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

    ++CreatedObjects_;

    if (hintId) {
        return hintId;
    }

    auto* hydraContext = GetCurrentHydraContext();
    auto version = hydraContext->GetVersion();
    auto hash = hydraContext->RandomGenerator()->Generate<ui32>();

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto cellTag = multicellManager->GetCellTag();

    // NB: The highest 16 bits of hash are used for externalizing cell tag in
    // externalized transaction ids.
    // NB: System transactions are not externalizeable.
    if (type == EObjectType::Transaction || type == EObjectType::NestedTransaction) {
        hash &= 0xffff;
    }

    auto result = MakeRegularId(type, cellTag, version, hash);

    if (auto* mutationContext = TryGetCurrentMutationContext()) {
        mutationContext->CombineStateHash(result);
    }

    return result;
}

int TObjectManager::RefObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(!object->IsGhost());
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    YT_LOG_DEBUG("Object referenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        object->GetObjectEphemeralRefCounter(),
        object->GetObjectWeakRefCounter());

    if (object->GetLifeStage() >= EObjectLifeStage::RemovalPreCommitted) {
        YT_LOG_DEBUG("Object referenced after its removal has been pre-committed (ObjectId: %v, LifeStage: %v)",
            object->GetId(),
            object->GetLifeStage());
    }

    if (refCounter == 1) {
        GarbageCollector_->UnregisterZombie(object);
    }

    return refCounter;
}

int TObjectManager::UnrefObject(TObject* object, int count)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(IsObjectAlive(object));
    YT_ASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject(count);
    YT_LOG_DEBUG("Object unreferenced (Id: %v, RefCounter: %v, EphemeralRefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        object->GetObjectEphemeralRefCounter(),
        object->GetObjectWeakRefCounter());

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

int TObjectManager::EphemeralRefObject(TObject* object)
{
    return GarbageCollector_->EphemeralRefObject(object);
}

void TObjectManager::EphemeralUnrefObject(TObject* object)
{
    return GarbageCollector_->EphemeralUnrefObject(object);
}

void TObjectManager::EphemeralUnrefObject(TObject* object, TEpoch epoch)
{
    GarbageCollector_->EphemeralUnrefObject(object, epoch);
}

int TObjectManager::WeakRefObject(TObject* object)
{
    return GarbageCollector_->WeakRefObject(object);
}

int TObjectManager::WeakUnrefObject(TObject* object)
{
    return GarbageCollector_->WeakUnrefObject(object);
}

void TObjectManager::SaveKeys(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveKeys(context);
    GarbageCollector_->SaveKeys(context);
    MutationIdempotizer_->Save(context);
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
    MutationIdempotizer_->Load(context);
}

void TObjectManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadValues(context);

    InitSchemas();

    GarbageCollector_->LoadValues(context);

    ResetTransactionAcls_ = context.GetVersion() < EMasterReign::FixTransactionACLs;
    DropLegacyClusterNodeMap_ = context.GetVersion() < EMasterReign::DropLegacyClusterNodeMap;
}

void TObjectManager::OnAfterSnapshotLoaded()
{
    if (ResetTransactionAcls_) {
        for (auto type : {
            EObjectType::Transaction,
            EObjectType::ExternalizedTransaction,
            EObjectType::SystemTransaction,
            EObjectType::UploadTransaction,
            EObjectType::NestedTransaction,
            EObjectType::ExternalizedNestedTransaction,
            EObjectType::SystemNestedTransaction,
            EObjectType::UploadNestedTransaction})
        {
            auto* schema = GetSchema(type);
            auto* acd = GetHandler(schema)->FindAcd(schema);

            YT_VERIFY(acd);
            acd->Clear();

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            acd->AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetEveryoneGroup(),
                EPermission::Read));
            acd->AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Remove));
            acd->AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Write));
            acd->AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Create));
        }
    }

    if (DropLegacyClusterNodeMap_) {
        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        auto id = MakeSchemaObjectId(EObjectType(804), primaryCellTag);
        SchemaMap_.Remove(id);
    }
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::Clear();

    MasterObject_ = TPoolAllocator::New<TMasterObject>(MasterObjectId_);
    MasterObject_->RefObject();

    MasterProxy_ = GetHandler(EObjectType::Master)->GetProxy(MasterObject_.get(), nullptr);

    SchemaMap_.Clear();

    InitSchemas();

    CreatedObjects_ = 0;
    DestroyedObjects_ = 0;

    ResetTransactionAcls_ = false;
    DropLegacyClusterNodeMap_ = false;

    GarbageCollector_->Clear();
    MutationIdempotizer_->Clear();
}

void TObjectManager::InitSchemas()
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
            auto schemaObject = TPoolAllocator::New<TSchemaObject>(id);
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
    TMasterAutomatonPart::OnRecoveryStarted();

    BufferedProducer_->SetEnabled(false);

    GarbageCollector_->Reset();
}

void TObjectManager::OnRecoveryComplete()
{
    TMasterAutomatonPart::OnRecoveryComplete();

    BufferedProducer_->SetEnabled(true);
}

void TObjectManager::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnLeaderActive();

    GarbageCollector_->Start();
    MutationIdempotizer_->Start();
}

void TObjectManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnStopLeading();

    GarbageCollector_->Stop();
    MutationIdempotizer_->Stop();
}

void TObjectManager::OnStartFollowing()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnStartFollowing();

    GarbageCollector_->Start();
}

void TObjectManager::OnStopFollowing()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnStopFollowing();

    GarbageCollector_->Stop();
}

void TObjectManager::CheckInvariants()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::CheckInvariants();

    for (const auto& [type, entry] : TypeToEntry_) {
        const auto& typeHandler = entry.Handler;
        typeHandler->CheckInvariants(Bootstrap_);
    }
}

TObject* TObjectManager::FindObject(TObjectId id)
{
    Bootstrap_->VerifyPersistentStateRead();

    const auto& handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->FindObject(id);
}

TObject* TObjectManager::GetObject(TObjectId id)
{
    Bootstrap_->VerifyPersistentStateRead();

    auto* object = FindObject(id);
    YT_VERIFY(object);
    return object;
}

TObject* TObjectManager::GetObjectOrThrow(TObjectId id)
{
    Bootstrap_->VerifyPersistentStateRead();

    auto* object = FindObject(id);
    if (!IsObjectAlive(object)) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "No such object %v",
            id);
    }

    return object;
}

TObject* TObjectManager::GetWeakGhostObject(TObjectId id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    return GarbageCollector_->GetWeakGhostObject(id);
}

void TObjectManager::RemoveObject(TObject* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (object->GetLifeStage() != EObjectLifeStage::CreationCommitted) {
        THROW_ERROR_EXCEPTION("Object life stage is %Qlv",
            object->GetLifeStage())
            << TErrorAttribute("object_id", object->GetId());
    }

    const auto& handler = GetHandler(object);
    if (None(handler->GetFlags() & ETypeFlags::Removable)) {
        THROW_ERROR_EXCEPTION("Object cannot be removed");
    }

    if (object->IsForeign()) {
        THROW_ERROR_EXCEPTION("Object is foreign");
    }

    auto objectRefCounter = object->GetObjectRefCounter(/*flushUnrefs*/ true);

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (multicellManager->IsPrimaryMaster() &&
        Any(handler->GetFlags() & ETypeFlags::TwoPhaseRemoval))
    {
        YT_LOG_DEBUG("Two-phase object removal started (ObjectId: %v, RefCounter: %v)",
            object->GetId(),
            objectRefCounter);

        object->SetLifeStage(EObjectLifeStage::RemovalStarted);
        object->ResetLifeStageVoteCount();

        NProto::TReqRemoveForeignObject request;
        ToProto(request.mutable_object_id(), object->GetId());
        multicellManager->PostToSecondaryMasters(request);

        CheckRemovingObjectRefCounter(object);
    } else {
        if (objectRefCounter != 1) {
            THROW_ERROR_EXCEPTION("Object is in use");
        }

        object->SetLifeStage(EObjectLifeStage::RemovalCommitted);
        DoRemoveObject(object);
    }
}

IYPathServicePtr TObjectManager::CreateRemoteProxy(TObjectId id)
{
    return New<TRemoteProxy>(Bootstrap_, id);
}

IYPathServicePtr TObjectManager::CreateRemoteProxy(TCellTag cellTag)
{
    return New<TRemoteProxy>(Bootstrap_, cellTag);
}

IObjectProxyPtr TObjectManager::GetProxy(
    TObject* object,
    TTransaction* transaction)
{
    Bootstrap_->VerifyPersistentStateRead();

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

void TObjectManager::BranchAttributes(
    const TObject* /*originatingObject*/,
    TObject* /*branchedObject*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
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

void TObjectManager::FillAttributes(
    TObject* object,
    const IAttributeDictionary& attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto pairs = attributes.ListPairs();
    if (pairs.empty()) {
        return;
    }

    auto proxy = GetProxy(object, nullptr);
    Sort(pairs, [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    for (const auto& [key, value] : pairs) {
        proxy->MutableAttributes()->Set(key, value);
    }
}

std::unique_ptr<TMutation> TObjectManager::CreateExecuteMutation(
    const IYPathServiceContextPtr& context,
    const TAuthenticationIdentity& identity)
{
    NProto::TReqExecute request;
    WriteAuthenticationIdentityToProto(&request, identity);

    auto requestMessage = context->GetRequestMessage();
    for (const auto& part : requestMessage) {
        request.add_request_parts(part.Begin(), part.Size());
    }

    auto mutation = CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request);
    mutation->SetHandler(BIND_NO_PROPAGATE(
        &TObjectManager::HydraExecuteLeader,
        MakeStrong(this),
        identity,
        MakeCodicilData(identity),
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

TObject* TObjectManager::CreateObject(
    TObjectId hintId,
    EObjectType type,
    IAttributeDictionary* attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = GetHandlerOrThrow(type);
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
    if (schema && schema->GetAttributes()) {
        attributeHolder = CreateEphemeralAttributes();
        for (const auto& [key, value] : schema->GetAttributes()->Attributes()) {
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

    // COMPAT(aleksandra-zh): medium replication hotfix.
    if (replicate && IsMediumType(type)) {
        auto* medium = object->As<TMedium>();
        replicatedAttributes->Set("index", medium->GetIndex());
    }

    if (multicellManager->IsMulticell() && Any(flags & ETypeFlags::TwoPhaseCreation)) {
        object->SetLifeStage(EObjectLifeStage::CreationStarted);
        object->ResetLifeStageVoteCount();
        YT_VERIFY(object->IncrementLifeStageVoteCount() == 1);
        YT_LOG_DEBUG("Two-phase object creation started (ObjectId: %v)",
            object->GetId());
    } else {
        object->SetLifeStage(EObjectLifeStage::CreationCommitted);
    }

    YT_VERIFY(object->GetObjectRefCounter() == 1);

    if (object->GetNativeCellTag() != multicellManager->GetCellTag() && !IsAlienType(type)) {
        object->SetForeign();
    }

    if (auto lifeStage = attributes->Find<EObjectLifeStage>("life_stage")) {
        attributes->Remove("life_stage");
        object->SetLifeStage(*lifeStage);
    }

    try {
        FillAttributes(object, *attributes);
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Failed to fill object attributes (ObjectId: %v)",
            object->GetId());
        // TODO(babenko): think of better way
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
            /*fallthrough*/

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

bool TObjectManager::IsObjectLifeStageValid(const TObject* object) const
{
    YT_VERIFY(IsObjectAlive(object));

    if (IsObjectLifeStageValidationSuppressed()) {
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

void TObjectManager::ValidateObjectLifeStage(const TObject* object) const
{
    if (!IsObjectLifeStageValid(object)) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::InvalidObjectLifeStage,
            "%v cannot be used since it is in %Qlv life stage",
            object->GetCapitalizedObjectName(),
            object->GetLifeStage());
    }
}

std::optional<TObject*> TObjectManager::FindObjectByAttributes(
    EObjectType type,
    const IAttributeDictionary* attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = GetHandlerOrThrow(type);
    return handler->FindObjectByAttributes(attributes);
}

void TObjectManager::ConfirmObjectLifeStageToPrimaryMaster(TObject* object)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (!multicellManager->IsSecondaryMaster()) {
        return;
    }

    auto lifeStage = object->GetLifeStage();
    if (lifeStage == EObjectLifeStage::CreationCommitted ||
        lifeStage == EObjectLifeStage::RemovalCommitted)
    {
        return;
    }

    YT_LOG_DEBUG("Confirming object life stage to primary master (ObjectId: %v, LifeStage: %v)",
        object->GetId(),
        lifeStage);

    NProto::TReqConfirmObjectLifeStage request;
    ToProto(request.mutable_object_id(), object->GetId());
    request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));
    multicellManager->PostToPrimaryMaster(request);
}

void TObjectManager::AdvanceObjectLifeStageAtSecondaryMasters(NYT::NObjectServer::TObject* object)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    YT_LOG_DEBUG("Advancing object life stage at secondary masters (ObjectId: %v, LifeStage: %v)",
        object->GetId(),
        object->GetLifeStage());

    NProto::TReqAdvanceObjectLifeStage advanceRequest;
    ToProto(advanceRequest.mutable_object_id(), object->GetId());
    advanceRequest.set_new_life_stage(static_cast<int>(object->GetLifeStage()));
    multicellManager->PostToSecondaryMasters(advanceRequest);
}

TObject* TObjectManager::ResolvePathToObject(const TYPath& path, TTransaction* transaction, const TResolvePathOptions& options)
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
    });
    const auto* payload = std::get_if<TPathResolver::TLocalObjectPayload>(&result.Payload);
    if (!payload) {
        THROW_ERROR_EXCEPTION("%v is not a local object",
            path);
    }

    return payload->Object;
}

auto TObjectManager::ResolveObjectIdsToPaths(const std::vector<TVersionedObjectId>& objectIds)
    -> TFuture<std::vector<TErrorOr<TVersionedObjectPath>>>
{
    // Request object paths from the primary cell.
    auto proxy = CreateObjectServiceReadProxy(
        Bootstrap_->GetRootClient(),
        NApi::EMasterChannelKind::Follower);

    // TODO(babenko): improve
    auto batchReq = proxy.ExecuteBatch();
    for (const auto& versionedId : objectIds) {
        auto req = TCypressYPathProxy::Get(FromObjectId(versionedId.ObjectId) + "/@path");
        SetTransactionId(req, versionedId.TransactionId);
        batchReq->AddRequest(req);
    }

    return
        batchReq->Invoke()
        .Apply(BIND([=] (const TErrorOr<TObjectServiceProxy::TRspExecuteBatchPtr>& batchRspOrError) -> TErrorOr<std::vector<TErrorOr<TVersionedObjectPath>>> {
            if (!batchRspOrError.IsOK()) {
                return TError("Error requesting object paths")
                    << batchRspOrError;
            }

            const auto& batchRsp = batchRspOrError.Value();
            auto rspOrErrors = batchRsp->GetResponses<TCypressYPathProxy::TRspGet>();
            YT_VERIFY(rspOrErrors.size() == objectIds.size());

            std::vector<TErrorOr<TVersionedObjectPath>> results;
            results.reserve(rspOrErrors.size());
            for (int index = 0; index < std::ssize(rspOrErrors); ++index) {
                const auto& rspOrError = rspOrErrors[index];
                if (rspOrError.IsOK()) {
                    const auto& rsp = rspOrError.Value();
                    results.push_back(TVersionedObjectPath{ConvertTo<TYPath>(TYsonString(rsp->value())), objectIds[index].TransactionId});
                } else {
                    results.push_back(TError(rspOrError));
                }
            }

            return results;
        }));
}

void TObjectManager::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
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
                "Prerequisite check failed: node %v revision mismatch: expected %x, found %x",
                path,
                revision,
                trunkNode->GetRevision());
        }
    }
}

TFuture<TSharedRefArray> TObjectManager::ForwardObjectRequest(
    const TSharedRefArray& requestMessage,
    TCellTag cellTag,
    NApi::EMasterChannelKind channelKind)
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

    auto identity = ParseAuthenticationIdentityFromProto(header);

    auto forwardedRequestMessage = SetRequestHeader(requestMessage, header);

    auto proxy = TObjectServiceProxy(
        Bootstrap_->GetClusterConnection(),
        channelKind,
        cellTag,
        /*stickyGroupSizeCache*/ nullptr);
    auto batchReq = proxy.ExecuteBatchNoBackoffRetries();
    batchReq->SetOriginalRequestId(requestId);
    batchReq->SetTimeout(timeout);
    batchReq->AddRequestMessage(std::move(forwardedRequestMessage));
    SetAuthenticationIdentity(batchReq, identity);

    YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v.%v, Path: %v, %v, Mutating: %v, "
        "CellTag: %v, ChannelKind: %v)",
        requestId,
        batchReq->GetRequestId(),
        header.service(),
        header.method(),
        ypathExt.target_path(),
        identity,
        ypathExt.mutating(),
        cellTag,
        channelKind);

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

void TObjectManager::ReplicateObjectCreationToSecondaryMaster(
    TObject* object,
    TCellTag cellTag)
{
    if (!IsObjectAlive(object)) {
        return;
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (object->IsBuiltin()) {
        // Builtin objects are already created at secondary masters,
        // so we just replicate mandatory attributes here.
        DoReplicateObjectAttributesToSecondaryMaster(object, cellTag, /*mandatory*/ true);
    } else {
        NProto::TReqCreateForeignObject request;
        ToProto(request.mutable_object_id(), object->GetId());
        request.set_type(static_cast<int>(object->GetType()));
        auto replicatedAttributes = GetReplicatedAttributes(object, /*mandatory*/ true, /*writableOnly*/ false);
        ToProto(request.mutable_object_attributes(), *replicatedAttributes);

        multicellManager->PostToMaster(request, cellTag);
    }
}

void TObjectManager::ReplicateObjectAttributesToSecondaryMaster(
    TObject* object,
    TCellTag cellTag)
{
    if (!IsObjectAlive(object)) {
        return;
    }

    DoReplicateObjectAttributesToSecondaryMaster(object, cellTag, /*mandatory*/ false);
}

TString TObjectManager::MakeCodicilData(const TAuthenticationIdentity& identity)
{
    return ToString(identity);
}

void TObjectManager::HydraExecuteLeader(
    const TAuthenticationIdentity& identity,
    const TString& codicilData,
    const IYPathServiceContextPtr& rpcContext,
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
        if (auto setResponseKeeperPromise = responseKeeper->EndRequest(mutationId, NRpc::CreateErrorResponseMessage(errorResponse))) {
            setResponseKeeperPromise();
        }

        YT_LOG_WARNING("Duplicate mutation application skipped (MutationId: %v)",
            mutationId);

        return;
    }

    std::optional<NTracing::TChildTraceContextGuard> traceContextGuard;
    if (auto* traceContext = NTracing::TryGetCurrentTraceContext()) {
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
        rpcContext->Reply(TError(ex));
    }

    if (!IsRecovery()) {
        securityManager->ChargeUser(user, {EUserWorkloadType::Write, 1, timer.GetElapsedTime()});
    }

    if (mutationId && !mutationContext->GetResponseKeeperSuppressed()) {
        MutationIdempotizer_->SetMutationApplied(mutationId);

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& responseKeeper = hydraFacade->GetResponseKeeper();
        // NB: Context must already be replied by now.
        const auto& error = rpcContext->GetError();
        if (auto setResponseKeeperPromise = error.IsOK()
            ? responseKeeper->EndRequest(mutationId, rpcContext->GetResponseMessage())
            : responseKeeper->EndRequest(mutationId, CreateErrorResponseMessage(error)))
        {
            setResponseKeeperPromise();
        }
    }
}

void TObjectManager::HydraExecuteFollower(NProto::TReqExecute* request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TSharedRefArrayBuilder requestMessageBuilder(request->request_parts_size());
    for (const auto& part : request->request_parts()) {
        requestMessageBuilder.Add(TSharedRef::FromString(part));
    }

    auto requestMessage = requestMessageBuilder.Finish();

    auto context = CreateYPathContext(
        std::move(requestMessage),
        ObjectServerLogger,
        NLogging::ELogLevel::Debug);

    auto identity = ParseAuthenticationIdentityFromProto(*request);

    auto codicilData = MakeCodicilData(identity);

    HydraExecuteLeader(identity, codicilData, context, GetCurrentMutationContext());
}

TFuture<void> TObjectManager::DestroyObjects(std::vector<TObjectId> objectIds)
{
    const auto& sequoiaConfig = Bootstrap_->GetConfigManager()->GetConfig()->SequoiaManager;

    NProto::TReqDestroyObjects sequoiaRequest;
    NProto::TReqDestroyObjects nonSequoiaRequest;
    for (auto objectId : objectIds) {
        const auto& handler = GetHandler(TypeFromId(objectId));
        auto* object = handler->FindObject(objectId);
        if (!object) {
            // Looks ok.
            continue;
        }
        if (sequoiaConfig->Enable && IsSequoiaId(objectId)) {
            ToProto(sequoiaRequest.add_object_ids(), objectId);
        } else {
            ToProto(nonSequoiaRequest.add_object_ids(), objectId);
        }
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(2);
    if (!nonSequoiaRequest.object_ids().empty()) {
        futures.push_back(CreateDestroyObjectsMutation(nonSequoiaRequest)
            ->CommitAndLog(Logger)
            .AsVoid());
    }

    if (!sequoiaRequest.object_ids().empty()) {
        futures.push_back(DestroySequoiaObjects(sequoiaRequest));
    }

    return AllSucceeded(std::move(futures))
        .AsVoid();
}

void TObjectManager::HydraPrepareDestroyObjects(
    TTransaction* /*transaction*/,
    NProto::TReqDestroyObjects* request,
    const NTransactionSupervisor::TTransactionPrepareOptions& options)
{
    YT_VERIFY(options.Persistent);
    YT_VERIFY(options.LatePrepare);

    for (auto protoId : request->object_ids()) {
        auto id = FromProto<TObjectId>(protoId);
        auto type = TypeFromId(id);
        const auto& handler = GetHandler(type);
        auto* object = handler->FindObject(id);
        if (!object || object->GetObjectRefCounter(/*flushUnrefs*/ true) > 0) {
            THROW_ERROR_EXCEPTION("Object %v is no more a zombie", object->GetId());
        }
    }

    DoDestroyObjects(request);
}

TFuture<void> TObjectManager::DestroySequoiaObjects(NProto::TReqDestroyObjects request)
{
    return Bootstrap_
        ->GetSequoiaClient()
        ->StartTransaction()
        .Apply(BIND([request = std::move(request), this, this_ = MakeStrong(this)] (const ISequoiaTransactionPtr& transaction) mutable {
            for (auto protoId : request.object_ids()) {
                auto id = FromProto<TObjectId>(protoId);
                auto type = TypeFromId(id);
                const auto& handler = GetHandler(type);
                auto* object = handler->FindObject(id);
                if (!object) {
                    continue;
                }
                handler->DestroySequoiaObject(object, transaction);
            }

            transaction->AddTransactionAction(
                Bootstrap_->GetCellTag(),
                NTransactionClient::MakeTransactionActionData(request));

            NApi::TTransactionCommitOptions commitOptions{
                .CoordinatorCellId = Bootstrap_->GetCellId(),
                .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
            };

            return transaction->Commit(commitOptions);
        }).AsyncVia(EpochAutomatonInvoker_));
}

void TObjectManager::DoDestroyObjects(NProto::TReqDestroyObjects* request) noexcept
{
    YT_VERIFY(HasMutationContext());

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
        if (!object || object->GetObjectRefCounter(/*flushUnrefs*/ true) > 0) {
            continue;
        }

        if (object->IsForeign() && object->GetImportRefCounter() > 0) {
            auto& crossCellRequest = getCrossCellRequest(id);
            crossCellRequest.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));
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

        YT_LOG_DEBUG("Object destroyed (Type: %v, Id: %v)",
            type,
            id);
    }

    for (const auto& [cellTag, perCellRequest] : crossCellRequestMap) {
        multicellManager->PostToMaster(perCellRequest, cellTag);
        YT_LOG_DEBUG("Requesting to unreference imported objects (CellTag: %v, Count: %v)",
            cellTag,
            perCellRequest.entries_size());
    }

    GarbageCollector_->CheckEmpty();
}

void TObjectManager::HydraDestroyObjects(NProto::TReqDestroyObjects* request)
{
    DoDestroyObjects(request);
}

void TObjectManager::HydraCreateForeignObject(NProto::TReqCreateForeignObject* request) noexcept
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

    YT_LOG_DEBUG("Foreign object created (Id: %v, Type: %v)",
        objectId,
        type);
}

void TObjectManager::HydraRemoveForeignObject(NProto::TReqRemoveForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());

    auto* object = FindObject(objectId);
    if (!IsObjectAlive(object)) {
        YT_LOG_DEBUG("Attempt to remove a non-existing foreign object (ObjectId: %v)",
            objectId);
        return;
    }

    if (object->GetLifeStage() != EObjectLifeStage::CreationCommitted) {
        YT_LOG_ALERT(
            "Requested to remove a foreign object with inappropriate life stage (ObjectId: %v, LifeStage: %v)",
            objectId,
            object->GetLifeStage());
        return;
    }

    YT_LOG_DEBUG("Removing foreign object (ObjectId: %v, RefCounter: %v)",
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

void TObjectManager::HydraUnrefExportedObjects(NProto::TReqUnrefExportedObjects* request) noexcept
{
    auto cellTag = FromProto<TCellTag>(request->cell_tag());

    for (const auto& entry : request->entries()) {
        auto objectId = FromProto<TObjectId>(entry.object_id());
        auto importRefCounter = entry.import_ref_counter();

        auto* object = FindObject(objectId);

        if (!IsObjectAlive(object)) {
            YT_LOG_ALERT(
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

    YT_LOG_DEBUG("Exported objects unreferenced (CellTag: %v, Count: %v)",
        cellTag,
        request->entries_size());
}

void TObjectManager::HydraConfirmObjectLifeStage(NProto::TReqConfirmObjectLifeStage* confirmRequest) noexcept
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    auto objectId = FromProto<TObjectId>(confirmRequest->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG("A non-existing object creation confirmed by secondary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto voteCount = object->IncrementLifeStageVoteCount();
    YT_LOG_DEBUG(
        "Object life stage confirmed by secondary cell (ObjectId: %v, CellTag: %v, VoteCount: %v)",
        objectId,
        confirmRequest->cell_tag(),
        voteCount);

    CheckObjectLifeStageVoteCount(object);
}

void TObjectManager::HydraAdvanceObjectLifeStage(NProto::TReqAdvanceObjectLifeStage* request) noexcept
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsSecondaryMaster());

    auto objectId = FromProto<TObjectId>(request->object_id());
    auto* object = FindObject(objectId);
    if (!object) {
        YT_LOG_DEBUG(
            "Life stage advancement for a non-existing object requested by the primary cell (ObjectId: %v)",
            objectId);
        return;
    }

    if (object->IsNative()) {
        YT_LOG_DEBUG(
            "Life stage advancement for a non-foreign object requested by primary cell (ObjectId: %v)",
            objectId);
        return;
    }

    auto oldLifeStage = object->GetLifeStage();
    auto newLifeStage = CheckedEnumCast<EObjectLifeStage>(request->new_life_stage());
    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG(
        "Object life stage advanced by primary cell (ObjectId: %v, LifeStage: %v -> %v)",
        objectId,
        oldLifeStage,
        newLifeStage);

    ConfirmObjectLifeStageToPrimaryMaster(object);

    if (newLifeStage == EObjectLifeStage::RemovalCommitted) {
        DoRemoveObject(object);
    }
}

void TObjectManager::HydraConfirmRemovalAwaitingCellsSyncObjects(NProto::TReqConfirmRemovalAwaitingCellsSyncObjects* request) noexcept
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    YT_VERIFY(multicellManager->IsPrimaryMaster());

    auto objectIds = FromProto<std::vector<TObjectId>>(request->object_ids());
    for (auto objectId : objectIds) {
        auto* object = FindObject(objectId);
        if (!object) {
            YT_LOG_ALERT(
                "Cells sync confirmed for a non-existing object (ObjectId: %v)",
                objectId);
            continue;
        }

        auto oldLifeStage = object->GetLifeStage();
        if (oldLifeStage != EObjectLifeStage::RemovalAwaitingCellsSync) {
            YT_LOG_ALERT(
                "Cells sync confirmed for an object with invalid life stage (ObjectId: %v, LifeStage: %v)",
                objectId,
                oldLifeStage);
            continue;
        }

        auto newLifeStage = EObjectLifeStage::RemovalCommitted;
        object->SetLifeStage(newLifeStage);

        YT_LOG_DEBUG(
            "Object life stage advanced after cells sync (ObjectId: %v, LifeStage: %v -> %v)",
            objectId,
            oldLifeStage,
            newLifeStage);

        AdvanceObjectLifeStageAtSecondaryMasters(object);
        GarbageCollector_->UnregisterRemovalAwaitingCellsSyncObject(object);
        DoRemoveObject(object);
    }
}

void TObjectManager::HydraRemoveExpiredRecentlyAppliedMutationIds(NProto::TReqRemoveExpiredRecentlyAppliedMutationIds* /*request*/)
{
    MutationIdempotizer_->RemoveExpiredMutations();
}

void TObjectManager::DoRemoveObject(TObject* object)
{
    YT_LOG_DEBUG("Object removed (ObjectId: %v)",
        object->GetId());
    FlushObjectUnrefs();
    if (auto refCounter = UnrefObject(object); refCounter != 0) {
        YT_LOG_ALERT(
            "Non-zero reference counter after object removal (ObjectId: %v, RefCounter: %v)",
            object->GetId(),
            refCounter);
    }
}

void TObjectManager::CheckRemovingObjectRefCounter(TObject* object)
{
    if (object->GetObjectRefCounter(/*flushUnrefs*/ true) != 1) {
        return;
    }

    auto oldLifeStage = object->GetLifeStage();
    YT_VERIFY(oldLifeStage == EObjectLifeStage::RemovalStarted);
    auto newLifeStage = EObjectLifeStage::RemovalPreCommitted;

    object->SetLifeStage(newLifeStage);

    YT_LOG_DEBUG("Object references released (ObjectId: %v, LifeStage: %v -> %v)",
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

void TObjectManager::CheckObjectLifeStageVoteCount(NYT::NObjectServer::TObject* object)
{
    while (true) {
        auto voteCount = object->GetLifeStageVoteCount();
        const auto& secondaryCellTags = Bootstrap_->GetMulticellManager()->GetSecondaryCellTags();
        YT_VERIFY(voteCount <= std::ssize(secondaryCellTags) + 1);
        if (voteCount < std::ssize(secondaryCellTags) + 1) {
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
                YT_LOG_ALERT("Unexpected object life stage (ObjectId: %v, LifeStage: %v)",
                    object->GetId(),
                    oldLifeStage);
                return;
        }

        object->SetLifeStage(newLifeStage);
        object->ResetLifeStageVoteCount();

        YT_LOG_DEBUG("Object life stage votes collected; advancing life stage (ObjectId: %v, LifeStage: %v -> %v)",
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

NProfiling::TTimeCounter* TObjectManager::GetMethodCumulativeExecuteTimeCounter(
    EObjectType type,
    const TString& method)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto key = std::pair(type, method);
    auto [entryPtr, inserted] = MethodToEntry_
        .FindOrInsert(
            key,
            [&] {
                auto entry = std::make_unique<TMethodEntry>();
                entry->CumulativeExecuteTimeCounter = ObjectServerProfiler
                    .WithTag("type", FormatEnum(type))
                    .WithTag("method", method)
                    .TimeCounter("/cumulative_execute_time");
                return entry;
            });
    return &(*entryPtr)->CumulativeExecuteTimeCounter;
}

const TGarbageCollectorPtr& TObjectManager::GetGarbageCollector() const
{
    Bootstrap_->VerifyPersistentStateRead();

    return GarbageCollector_;
}

void TObjectManager::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TSensorBuffer buffer;
    buffer.AddGauge("/zombie_object_count", GarbageCollector_->GetZombieCount());
    buffer.AddGauge("/ephemeral_ghost_object_count", GarbageCollector_->GetEphemeralGhostCount());
    buffer.AddGauge("/ephemeral_unref_queue_size", GarbageCollector_->GetEphemeralGhostUnrefQueueSize());
    buffer.AddGauge("/weak_ghost_object_count", GarbageCollector_->GetWeakGhostCount());
    buffer.AddGauge("/locked_object_count", GarbageCollector_->GetLockedCount());
    buffer.AddCounter("/created_objects", CreatedObjects_);
    buffer.AddCounter("/destroyed_objects", DestroyedObjects_);
    buffer.AddGauge("/recently_finished_mutation_count", MutationIdempotizer_->RecentlyFinishedMutationCount());
    BufferedProducer_->Update(std::move(buffer));
}

IAttributeDictionaryPtr TObjectManager::GetReplicatedAttributes(
    TObject* object,
    bool mandatory,
    bool writableOnly)
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
    THashSet<TString> systemAttributeKeys;
    for (const auto& descriptor : descriptors) {
        systemAttributeKeys.insert(descriptor.InternedKey.Unintern());

        if (!descriptor.Replicated) {
            continue;
        }

        if (descriptor.Mandatory != mandatory) {
            continue;
        }

        if (!descriptor.Writable && writableOnly) {
            continue;
        }

        auto value = proxy->FindBuiltinAttribute(descriptor.InternedKey);
        if (value) {
            const auto& key = descriptor.InternedKey.Unintern();
            replicateKey(key, value);
        }
    }

    // Custom attributes aren't supposed to be replicated when replicating object creation.
    if (!mandatory) {
        // Check custom attributes.
        if (const auto* customAttributes = object->GetAttributes()) {
            for (const auto& [key, value] : object->GetAttributes()->Attributes()) {
                // Never replicate custom attributes overlaid by system ones.
                if (systemAttributeKeys.contains(key)) {
                    continue;
                }

                replicateKey(key, value);
            }
        }
    }

    return attributes;
}

void TObjectManager::DoReplicateObjectAttributesToSecondaryMaster(
    TObject* object,
    TCellTag cellTag,
    bool mandatory)
{
    auto request = TYPathProxy::Set(FromObjectId(object->GetId()) + "/@");
    auto replicatedAttributes = GetReplicatedAttributes(object, mandatory, /*writableOnly*/ true);
    request->set_value(ConvertToYsonString(replicatedAttributes->ToMap()).ToString());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(request, cellTag);
}

void TObjectManager::OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
{
    auto schemas = GetValuesSortedByKey(SchemaMap_);
    for (auto* schema : schemas) {
        ReplicateObjectAttributesToSecondaryMaster(schema, cellTag);
    }
}

const TDynamicObjectManagerConfigPtr& TObjectManager::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->ObjectManager;
}

void TObjectManager::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    ProfilingExecutor_->SetPeriod(GetDynamicConfig()->ProfilingPeriod);
}

////////////////////////////////////////////////////////////////////////////////

IObjectManagerPtr CreateObjectManager(NCellMaster::TBootstrap* bootstrap)
{
    return New<TObjectManager>(bootstrap);
}

IObjectManagerPtr CreateObjectManager(TTestingTag tag, NCellMaster::TBootstrap* bootstrap)
{
    return New<TObjectManager>(tag, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

