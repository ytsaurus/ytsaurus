#include "object_service.h"

#include "private.h"

#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "helpers.h"
#include "master_connector.h"
#include "path_resolver.h"
#include "per_user_and_workload_request_queue_provider.h"
#include "sequoia_service.h"
#include "sequoia_session.h"
#include "response_keeper.h"
#include "user_directory.h"
#include "user_directory_synchronizer.h"

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/ypath_detail.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressClient::NProto;
using namespace NDistributedThrottler;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NRpc;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;
using NSequoiaClient::TRawYPath;

////////////////////////////////////////////////////////////////////////////////

class TObjectService
    : public IObjectService
    , public TServiceBase
{
public:
    explicit TObjectService(IBootstrap* bootstrap)
        : TServiceBase(
            /*invoker*/ nullptr,
            TObjectServiceProxy::GetDescriptor(),
            CypressProxyLogger(),
            TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
        , Connection_(bootstrap->GetNativeConnection())
        , ThreadPool_(CreateThreadPool(/*threadCount*/ 1, "ObjectService"))
        , Invoker_(ThreadPool_->GetInvoker())
        , ThrottlerFactory_(bootstrap->CreateDistributedThrottlerFactory(
            GetDynamicConfig()->DistributedThrottler,
            bootstrap->GetControlInvoker(),
            "/cypress_proxy/object_service",
            CypressProxyLogger(),
            CypressProxyProfiler()
                .WithDefaultDisabled()
                .WithSparse()
                .WithPrefix("/distributed_throttler")))
        , RequestQueueProvider_(New<TExecuteRequestQueueProvider>(
            CreateReconfigurationCallback(bootstrap, ThrottlerFactory_),
            /*owner*/ this))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(10'000)
            .SetConcurrencyLimit(10'000)
            .SetInvoker(Invoker_)
            .SetRequestQueueProvider(RequestQueueProvider_));

        DeclareServerFeature(EMasterFeature::Portals);
        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);

        const auto& userDirectorySynchronizer = bootstrap->GetUserDirectorySynchronizer();
        userDirectorySynchronizer->SubscribeUserDescriptorUpdated(
            BIND_NO_PROPAGATE(&TObjectService::OnUserDirectoryUpdated, MakeWeak(this)));

        const auto& configManager = Bootstrap_->GetDynamicConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TObjectService::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Reconfigure(const TObjectServiceDynamicConfigPtr& config) override
    {
        ThreadPool_->SetThreadCount(config->ThreadPoolSize);
    }

    IServicePtr GetService() override
    {
        return MakeStrong(this);
    }

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        return Bootstrap_->GetMasterConnector()->IsRegistered();
    }

    const TObjectServiceDynamicConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->ObjectService;
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    IBootstrap* const Bootstrap_;

    const NApi::NNative::IConnectionPtr Connection_;

    const IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;

    const IDistributedThrottlerFactoryPtr ThrottlerFactory_;
    const TPerUserAndWorkloadRequestQueueProviderPtr RequestQueueProvider_;

    class TExecuteSession;
    using TExecuteSessionPtr = TIntrusivePtr<TExecuteSession>;

    class TExecuteRequestQueueProvider
        : public TPerUserAndWorkloadRequestQueueProvider
    {
    public:
        TExecuteRequestQueueProvider(
            TReconfigurationCallback reconfigurationCallback,
            TObjectService* owner)
            : TPerUserAndWorkloadRequestQueueProvider(std::move(reconfigurationCallback))
            , Owner_(owner)
        { }

    private:
        TObjectService* const Owner_;

        TRequestQueuePtr CreateQueueForKey(const TKey& userNameAndWorkloadType) override
        {
            const auto& throttlerConfig = userNameAndWorkloadType.second == EUserWorkloadType::Read
                ? Owner_->GetDynamicConfig()->DefaultPerUserReadRequestWeightThrottler
                : Owner_->GetDynamicConfig()->DefaultPerUserWriteRequestWeightThrottler;
            auto queueName = GetRequestQueueNameForKey(userNameAndWorkloadType);
            auto throttlerId = GetDistributedWeightThrottlerId(queueName);

            return NRpc::CreateRequestQueue(
                queueName,
                userNameAndWorkloadType,
                // Bytes throttling is not supported.
                CreateNamedReconfigurableThroughputThrottler(
                    InfiniteRequestThrottlerConfig,
                    "BytesThrottler",
                    CypressProxyLogger()),
                Owner_->ThrottlerFactory_->GetOrCreateThrottler(
                    // TODO(babenko): migrate to std::string
                    TString(throttlerId),
                    throttlerConfig));
        }
    };

    static TPerUserAndWorkloadRequestQueueProvider::TReconfigurationCallback CreateReconfigurationCallback(
        IBootstrap* bootstrap,
        IDistributedThrottlerFactoryPtr throttlerFactory)
    {
        return BIND([
            bootstrap,
            throttlerFactory = std::move(throttlerFactory)
        ] (const TPerUserAndWorkloadRequestQueueProvider::TKey& userNameAndWorkloadType, const TRequestQueuePtr& queue)
        {
            const auto& dynamicConfig = bootstrap->GetDynamicConfigManager()->GetConfig()->ObjectService;
            if (!dynamicConfig->EnablePerUserRequestWeightThrottling) {
                queue->ConfigureWeightThrottler(nullptr);
                return;
            }

            // TODO(danilalexeev): Support queue size limit reconfiguration.
            const auto& userDirectory = bootstrap->GetUserDirectory();
            const auto descriptor = userDirectory->FindByName(userNameAndWorkloadType.first);
            if (!descriptor) {
                return;
            }

            auto newConfig = TThroughputThrottlerConfig::Create(GetUserRequestRateLimit(*descriptor, userNameAndWorkloadType.second));
            queue->ConfigureWeightThrottler(newConfig);

            // We utilize the fact that #GetOrCreateThrottle keeps #TWrappedThrottler pointers valid,
            // including the one inside the request queue.
            // TODO(danilalexeev): Implement public methods to explicitly set request queue's throttlers.
            auto queueName = GetRequestQueueNameForKey(userNameAndWorkloadType);
            auto throttlerId = GetDistributedWeightThrottlerId(queueName);
            // TODO(babenko): migrate to std::string
            throttlerFactory->GetOrCreateThrottler(TString(throttlerId), newConfig);
        });
    }

    void OnUserDirectoryUpdated(const std::string& userName)
    {
        RequestQueueProvider_->ReconfigureQueue({userName, EUserWorkloadType::Read});
        RequestQueueProvider_->ReconfigureQueue({userName, EUserWorkloadType::Write});
    }

    void OnDynamicConfigChanged(
        const TCypressProxyDynamicConfigPtr& oldConfig,
        const TCypressProxyDynamicConfigPtr& newConfig)
    {
        const auto& oldObjectServiceConfig = oldConfig->ObjectService;
        const auto& newObjectServiceConfig = newConfig->ObjectService;

        ThrottlerFactory_->Reconfigure(newObjectServiceConfig->DistributedThrottler);

        // Request queue provider's default configs are irrelevant in case of
        // distributed throttler, but we set it anyway here just in case.
        RequestQueueProvider_->UpdateDefaultConfigs({
            newObjectServiceConfig->DefaultPerUserWriteRequestWeightThrottler,
            /*BytesThrottlerConfig*/ InfiniteRequestThrottlerConfig});

        if (newObjectServiceConfig->EnablePerUserRequestWeightThrottling != oldObjectServiceConfig->EnablePerUserRequestWeightThrottling) {
            RequestQueueProvider_->UpdateThrottlingEnabledFlags(
                newObjectServiceConfig->EnablePerUserRequestWeightThrottling,
                /*enableBytesThrottling*/ false);
            RequestQueueProvider_->ReconfigureAllQueues();

            YT_LOG_DEBUG("Per-user request weight throttling was %v",
                newObjectServiceConfig->EnablePerUserRequestWeightThrottling ? "enabled" : "disabled");
        }
    }
};

using TObjectServicePtr = TIntrusivePtr<TObjectService>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestTarget,
    (Undetermined)
    (None) // Request is already executed or parse error occurred.
    (Master)
    (Sequoia)
);

////////////////////////////////////////////////////////////////////////////////

class TObjectService::TExecuteSession
    : public TRefCounted
{
public:
    TExecuteSession(
        TObjectServicePtr owner,
        TCtxExecutePtr rpcContext,
        TCellTag targetCellTag,
        EMasterChannelKind masterChannelKind)
        : Owner_(std::move(owner))
        , RpcContext_(std::move(rpcContext))
        , TargetCellTag_(targetCellTag)
        , MasterChannelKind_(masterChannelKind)
        , ForceUseTargetCellTag_(
            targetCellTag != Owner_->Bootstrap_->GetNativeConnection()->GetPrimaryMasterCellTag())
        , Logger(Owner_->Logger)
    { }

    void Run()
    {
        try {
            GuardedRun();
        } catch (const std::exception& ex) {
            Reply(TError(ex));
        }
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;

    const TCellTag TargetCellTag_;
    const EMasterChannelKind MasterChannelKind_;

    // If client's request is annotated with non-primary master cell tag client
    // already knows where its requests should be forwarded to. For such
    // requests Sequoia resolve should not affect the choice of master cell.
    //
    // The typical case is requesting chunk owner attributes from external
    // master cell: Sequoia can resolve object ID but can only forward it to
    // object's native cell.
    const bool ForceUseTargetCellTag_;

    struct TSubrequest
    {
        TSharedRefArray RequestMessage;
        std::optional<NRpc::NProto::TRequestHeader> RequestHeader;

        ERequestTarget Target = ERequestTarget::Undetermined;

        // If request was resolved in Sequoia and forwared to master server then
        // "No such object" error should be retriable to allow the following use
        // case:
        //
        // First client:
        //   now = datetime.now()
        //   while True:
        //     creation_time = get("//my/node/@creation_time")
        //     if creation_time >= now:
        //       break
        //
        // Second client:
        //   move("//my/node_tmp", "//my/node", force=True)
        TNodeId ResolvedNodeId;

        bool IsResolved() const
        {
            return static_cast<bool>(ResolvedNodeId);
        }
    };
    std::vector<TSubrequest> Subrequests_;

    const NLogging::TLogger Logger;

    void GuardedRun()
    {
        ParseSubrequests();

        if (Owner_->GetDynamicConfig()->AllowBypassMasterResolve) {
            PredictNonSequoia();
        } else {
            PredictNonMaster();
            InvokeMasterRequests(/*beforeSequoiaResolve*/ true);
        }

        InvokeSequoiaRequests();
        InvokeMasterRequests(/*beforeSequoiaResolve*/ false);

        Reply();
    }

    void ParseSubrequests()
    {
        const auto& request = RpcContext_->Request();
        const auto& attachments = RpcContext_->RequestAttachments();

        auto subrequestCount = request.part_counts_size();
        Subrequests_.resize(subrequestCount);

        int currentPartIndex = 0;
        std::optional<bool> mutating;
        for (int index = 0; index < subrequestCount; ++index) {
            auto& subrequest = Subrequests_[index];

            auto partCount = request.part_counts(index);
            TSharedRefArrayBuilder messageBuilder(partCount);
            for (int partIndex = 0; partIndex < partCount; ++partIndex) {
                messageBuilder.Add(attachments[currentPartIndex++]);
            }
            subrequest.RequestMessage = messageBuilder.Finish();

            // NB: request header is parsed twice for each subrequest: first
            // time to predict if it should be handled by master and second time
            // on sequoia service context creation. We consider such overhead
            // insignificant.
            auto& header = subrequest.RequestHeader.emplace();
            if (!TryParseRequestHeader(subrequest.RequestMessage, &header)) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Could not parse subrequest header")
                    << TErrorAttribute("subrequest_index", index);
            }

            const auto& ypathExt = header.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
            auto mutatingSubrequest = ypathExt.mutating();

            if (!mutating.has_value()) {
                mutating = mutatingSubrequest;
            }

            if (mutating != mutatingSubrequest && Owner_->GetDynamicConfig()->AlertOnMixedReadWriteBatch) {
                YT_LOG_ALERT("Batch request contains both mutating and non-mutating subrequests");
            }
        }
    }

    void PredictNonMaster()
    {
        for (int index = 0; index < std::ssize(Subrequests_); ++index) {
            auto& subrequest = Subrequests_[index];

            YT_VERIFY(subrequest.RequestHeader);
            YT_VERIFY(subrequest.Target == ERequestTarget::Undetermined);

            // If this is a rootstock creation request then don't bother master
            // with it.
            if (subrequest.RequestHeader->method() != "Create") {
                continue;
            }

            auto context = CreateSequoiaServiceContext(subrequest.RequestMessage);
            auto reqCreate = TryParseReqCreate(context);
            if (!reqCreate) {
                // Parse failure.
                subrequest.Target = ERequestTarget::None;
                ReplyOnSubrequest(index, context->GetResponseMessage());
                continue;
            }

            if (reqCreate->Type == EObjectType::Rootstock) {
                subrequest.Target = ERequestTarget::Sequoia;
            }
        }
    }

    void PredictNonSequoia()
    {
        for (int index = 0; index < std::ssize(Subrequests_); ++index) {
            auto& subrequest = Subrequests_[index];

            YT_VERIFY(subrequest.RequestHeader.has_value());
            YT_VERIFY(subrequest.Target == ERequestTarget::Undetermined);

            const auto& method = subrequest.RequestHeader->method();
            // Such requests already contain information about target cell
            // inside the TReqExecute message.
            if (IsMethodShouldBeHandledByMaster(method)) {
                subrequest.Target = ERequestTarget::Master;
            }
        }
    }

    void InvokeMasterRequests(bool beforeSequoiaResolve)
    {
        // Collect subrequest for each master cell.
        THashMap<TCellTag, std::vector<int>> cellTagToSubrequestIndices;
        for (int index = 0; index < std::ssize(Subrequests_); ++index) {
            const auto& subrequest = Subrequests_[index];
            auto target = Subrequests_[index].Target;

            if (target != ERequestTarget::Undetermined && target != ERequestTarget::Master) {
                // Request is either executed or should be executed in Sequoia.
                continue;
            }

            auto cellTag = TargetCellTag_;
            if (subrequest.IsResolved() && !ForceUseTargetCellTag_) {
                cellTag = CellTagFromId(subrequest.ResolvedNodeId);
            }

            cellTagToSubrequestIndices[cellTag].push_back(index);
        }

        if (cellTagToSubrequestIndices.empty()) {
            return;
        }

        struct TMasterRequestInfo
        {
            TCellTag CellTag;
            TRange<int> SubrequestIndices;
        };
        std::vector<TMasterRequestInfo> requestInfos;
        requestInfos.reserve(cellTagToSubrequestIndices.size());
        std::vector<TFuture<TObjectServiceProxy::TRspExecutePtr>> responseFutures;
        responseFutures.reserve(cellTagToSubrequestIndices.size());
        for (const auto& [cellTag, subrequestIndices] : cellTagToSubrequestIndices) {
            requestInfos.push_back({cellTag, subrequestIndices});
            responseFutures.push_back(InvokeMasterRequestsToCell(subrequestIndices, cellTag));
        }

        auto responsesOrError = WaitFor(AllSet(std::move(responseFutures)));
        if (!responsesOrError.IsOK()) {
            auto errorResponse = CreateErrorResponseMessage(
                TError("Error communicating with master") << std::move(responsesOrError));
            for (const auto& requestInfo : requestInfos) {
                ReplyOnSubrequests(requestInfo.SubrequestIndices, errorResponse);
            }
            return;
        }

        const auto& responses = responsesOrError.Value();
        for (const auto& [response, requestInfo] : Zip(responses, requestInfos)) {
            if (!response.IsOK()) {
                auto responseMessage = CreateErrorResponseMessage(
                    TError("Error communicating with master cell %v", requestInfo.CellTag)
                        << std::move(response));
                ReplyOnSubrequests(requestInfo.SubrequestIndices, responseMessage);
            } else {
                HandleMasterResponse(beforeSequoiaResolve, requestInfo.SubrequestIndices, response.Value());
            }
        }
    }

    TFuture<TObjectServiceProxy::TRspExecutePtr> InvokeMasterRequestsToCell(
        TRange<int> subrequestIndices,
        TCellTag cellTag)
    {
        const auto& connection = Owner_->Bootstrap_->GetNativeConnection();
        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(
            connection->GetMasterChannelOrThrow(MasterChannelKind_, cellTag));

        auto masterRequest = proxy.Execute();

        // Copy request.
        masterRequest->CopyFrom(RpcContext_->Request());

        // Copy authentication identity.
        SetAuthenticationIdentity(masterRequest, RpcContext_->GetAuthenticationIdentity());

        // Copy some header extensions.
        auto copyHeaderExtension = [&] (auto tag) {
            if (RpcContext_->RequestHeader().HasExtension(tag)) {
                const auto& ext = RpcContext_->RequestHeader().GetExtension(tag);
                masterRequest->Header().MutableExtension(tag)->CopyFrom(ext);
            }
        };
        copyHeaderExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);

        // Fill request with non-Sequoia requests.
        masterRequest->clear_part_counts();

        for (auto index : subrequestIndices) {
            const auto& subrequest = Subrequests_[index];
            const auto& requestMessage = subrequest.RequestMessage;
            masterRequest->add_part_counts(requestMessage.size());
            masterRequest->Attachments().insert(
                masterRequest->Attachments().end(),
                requestMessage.Begin(),
                requestMessage.End());
        }

        return masterRequest->Invoke();
    }

    void HandleMasterResponse(
        bool beforeSequoiaResolve,
        TRange<int> subrequestIndices,
        const TObjectServiceProxy::TRspExecutePtr& masterResponse)
    {
        int currentPartIndex = 0;
        for (const auto& subresponse : masterResponse->subresponses()) {
            auto partCount = subresponse.part_count();
            auto partsRange = TRange<TSharedRef>(
                masterResponse->Attachments().begin() + currentPartIndex,
                masterResponse->Attachments().begin() + currentPartIndex + partCount);
            currentPartIndex += partCount;

            auto index = subrequestIndices[subresponse.index()];

            TSharedRefArray subresponseMessage(partsRange, TSharedRefArray::TMoveParts{});

            if (beforeSequoiaResolve) {
                if (IsSubrequestRejectedByMaster(index, subresponseMessage)) {
                    YT_LOG_DEBUG(
                        "Subrequest was rejected by master server in favor of Sequoia "
                        "(RequestId: %v, SubrequestIndex: %v)",
                        RpcContext_->GetRequestId(),
                        index);

                    Subrequests_[index].Target = ERequestTarget::Sequoia;
                    continue;
                }
            } else {
                auto [patchedMessage, originError] = WrapRetriableResolveError(index, subresponseMessage);
                if (patchedMessage) {
                    YT_LOG_DEBUG(
                        originError,
                        "Possible Sequoia resolve miss encountered; marking it as retriable "
                        "(RequestId: %v, SubrequestIndex: %v, SequoiaObjectId: %v)",
                        RpcContext_->GetRequestId(),
                        index,
                        Subrequests_[index].ResolvedNodeId);
                    // See comment next to |TSubrequest::TResolvedNodeId|.
                    subresponseMessage = std::move(patchedMessage);
                }
            }

            Subrequests_[index].Target = ERequestTarget::None;
            ReplyOnSubrequest(index, std::move(subresponseMessage));
        }
    }

    bool IsSubrequestRejectedByMaster(
        int subrequestIndex,
        const TSharedRefArray& responseMessage)
    {
        auto header = ParseResponseHeader(subrequestIndex, responseMessage);
        if (!header.has_error()) {
            return false;
        }

        auto error = FromProto<TError>(header.error());
        return error
            .FindMatching(NObjectClient::EErrorCode::RequestInvolvesSequoia)
            .has_value();
    }

    // If resolve miss occurred patched response message and resolve error are
    // returned. Otherwise, empty array and OK are returned.
    std::pair<TSharedRefArray, TError> WrapRetriableResolveError(
        int subrequestIndex,
        const TSharedRefArray& responseMessage)
    {
        auto* subrequest = &Subrequests_[subrequestIndex];
        if (!subrequest->IsResolved()) {
            // Subrequest wasn't resolved in Sequoia.
            return {};
        }

        auto header = ParseResponseHeader(subrequestIndex, responseMessage);
        if (!header.has_error()) {
            return {};
        }

        auto originError = FromProto<TError>(header.error());

        auto noSuchObjectErrorMessage = Format("No such object %v", subrequest->ResolvedNodeId);

        auto noSuchObjectError = originError.FindMatching([&] (const TError& error) {
            if (error.GetCode() != NYTree::EErrorCode::ResolveError) {
                return false;
            }

            // TODO(kvk1920): design some way to avoid comparing full error
            // message.

            return error.GetMessage() == noSuchObjectErrorMessage;
        });

        if (!noSuchObjectError.has_value()) {
            return {};
        }

        ToProto(
            header.mutable_error(),
            TError(
                NSequoiaClient::EErrorCode::SequoiaRetriableError,
                "Object was resolved in Sequoia but missing on master")
                << originError);

        return {CreateErrorResponseMessage(header), std::move(originError)};
    }

    NRpc::NProto::TResponseHeader ParseResponseHeader(
        int subrequestIndex,
        const TSharedRefArray& responseMessage)
    {
        NRpc::NProto::TResponseHeader header;
        if (!TryParseResponseHeader(responseMessage, &header)) {
            auto error = TError(
                NRpc::EErrorCode::ProtocolError,
                "Error parsing response header")
                << TErrorAttribute("request_id", RpcContext_->GetRequestId())
                << TErrorAttribute("subrequest_index", subrequestIndex);
            YT_LOG_WARNING(error);

            THROW_ERROR error;
        }

        return header;
    }

    void ReplyOnSubrequests(TRange<int> subrequestIndices, TSharedRefArray responseMessage)
    {
        for (int index : subrequestIndices) {
            Subrequests_[index].Target = ERequestTarget::None;
            ReplyOnSubrequest(index, responseMessage);
        }
    }

    //! Rewrites subrequest header taking into account resolve result.
    /*!
     *  For Cypress resolve result we may want to rewrite target path in case of
     *  link resolution since it cannot be done without Sequoia tables.
     *
     *  For Sequoia resolve result we have to rewrite target path because right
     *  now method resolution in YTree relies on request header containing
     *  unresolved suffix as target path. Moreover, for the sake of futher
     *  forwarding to master we are setting "allow_resolve_from_sequoia_object"
     *  to |true|.
     */
    static void PatchRequestAfterResolve(
        TSubrequest* subrequest,
        const TResolveResult& resolveResult)
    {
        TStringBuf newPath;

        Visit(resolveResult,
            [&] (const TCypressResolveResult& cypressResolveResult) {
                newPath = cypressResolveResult.Path.Underlying();
            },
            [&] (const TSequoiaResolveResult& sequoiaResolveResult) {
                subrequest->ResolvedNodeId = sequoiaResolveResult.Id;

                newPath = sequoiaResolveResult.UnresolvedSuffix.Underlying();
            });

        auto& header = *subrequest->RequestHeader;
        SetAllowResolveFromSequoiaObject(&header, true);

        auto* ypathExt = header.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (newPath != ypathExt->target_path()) {
            if (!ypathExt->has_original_target_path()) {
                ypathExt->set_original_target_path(ypathExt->target_path());
            }

            ypathExt->set_target_path(ToProto<TProtobufString>(newPath));
        }

        subrequest->RequestMessage = SetRequestHeader(
            subrequest->RequestMessage,
            header);
    }

    static void RewriteRequestForForwardingToMaster(
        TSubrequest* subrequest,
        const TResolveResult& resolveResult)
    {
        YT_ASSERT(subrequest->RequestHeader.has_value());

        auto& header = *subrequest->RequestHeader;
        SetAllowResolveFromSequoiaObject(&header, true);

        // Replace "<unresolved-suffix>"" with "#<object-id>/<unresolved-suffix>".
        if (const auto* sequoiaResolveResult = std::get_if<TSequoiaResolveResult>(&resolveResult)) {
            auto* ypathExt = header.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
            ypathExt->set_target_path(
                FromObjectId(sequoiaResolveResult->Id) + sequoiaResolveResult->UnresolvedSuffix.Underlying());
        }

        subrequest->RequestMessage = SetRequestHeader(subrequest->RequestMessage, header);
    }

    //! Either executes subrequest in Sequoia or marks it as non-Sequoia. May
    //! alter subrequest message due to links resolution.
    std::optional<TSharedRefArray> ExecuteSequoiaSubrequest(TSubrequest* subrequest)
    {
        YT_VERIFY(
            subrequest->Target == ERequestTarget::Undetermined ||
            subrequest->Target == ERequestTarget::Sequoia);

        auto originalTargetPath = TRawYPath(GetRequestTargetYPath(*subrequest->RequestHeader));
        auto cypressTransactionId = GetTransactionId(*subrequest->RequestHeader);

        if (cypressTransactionId && !IsCypressTransactionType(TypeFromId(cypressTransactionId))) {
            // Requests with system transactions cannot be handled in Sequoia.
            subrequest->Target = ERequestTarget::Master;
            return std::nullopt;
        }

        TSequoiaSessionPtr session;
        TResolveResult resolveResult;
        try {
            session = TSequoiaSession::Start(Owner_->Bootstrap_, cypressTransactionId);
            resolveResult = ResolvePath(
                session,
                originalTargetPath,
                subrequest->RequestHeader->method());
        } catch (const std::exception& ex) {
            return CreateErrorResponseMessage(ex);
        }

        PatchRequestAfterResolve(subrequest, resolveResult);

        // NB: This can crash on invalid request header but it has been already
        // parsed before in order to predict if subrequest should be handled by
        // master.
        auto context = CreateSequoiaServiceContext(subrequest->RequestMessage);

        const auto& responseKeeper = Owner_->Bootstrap_->GetResponseKeeper();
        if (auto response = responseKeeper->FindResponse(context, session->SequoiaTransaction())) {
            session->Abort();
            return response;
        }

        auto invokeResult = CreateSequoiaService(Owner_->Bootstrap_)
            ->TryInvoke(context, session, resolveResult);
        switch (invokeResult) {
            case EInvokeResult::Executed:
                return context->GetResponseMessage();
            case EInvokeResult::ForwardToMaster:
                RewriteRequestForForwardingToMaster(subrequest, resolveResult);
                subrequest->Target = ERequestTarget::Master;
                return std::nullopt;
            default:
                YT_ABORT();
        }
    }

    void InvokeSequoiaRequests()
    {
        const auto& masterConnector = Owner_->Bootstrap_->GetMasterConnector();
        masterConnector->ValidateRegistration();

        for (int index = 0; index < std::ssize(Subrequests_); ++index) {
            auto& subrequest = Subrequests_[index];
            auto target = subrequest.Target;
            if (target != ERequestTarget::Undetermined && target != ERequestTarget::Sequoia) {
                continue;
            }

            YT_LOG_DEBUG("Executing subrequest in Sequoia (RequestId: %v, SubrequestIndex: %v)",
                RpcContext_->GetRequestId(),
                index);

            if (auto subresponse = ExecuteSequoiaSubrequest(&subrequest)) {
                subrequest.Target = ERequestTarget::None;
                ReplyOnSubrequest(index, std::move(*subresponse));
            }
        }
    }

    void Reply(const TError& error = {})
    {
        RpcContext_->Reply(error);
    }

    void ReplyOnSubrequest(int subrequestIndex, TSharedRefArray subresponseMessage)
    {
        // Caller is responsible for marking subrequest as executed.
        YT_VERIFY(Subrequests_[subrequestIndex].Target == ERequestTarget::None);

        auto& response = RpcContext_->Response();

        auto* subresponseInfo = response.add_subresponses();
        subresponseInfo->set_index(subrequestIndex);
        subresponseInfo->set_part_count(subresponseMessage.Size());
        response.Attachments().insert(
            response.Attachments().end(),
            subresponseMessage.Begin(),
            subresponseMessage.End());
    }

    bool CheckSubresponseError(const TSharedRefArray& message, TErrorCode errorCode)
    {
        try {
            auto subresponse = New<NYTree::TYPathResponse>();
            subresponse->Deserialize(message);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            return error.FindMatching(errorCode).has_value();
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    if (!request->has_cell_tag()) {
        THROW_ERROR_EXCEPTION("Cell tag is not provided in request");
    }

    if (!request->has_master_channel_kind()) {
        THROW_ERROR_EXCEPTION("Peer kind is not provided in request");
    }

    auto cellTag = FromProto<TCellTag>(request->cell_tag());
    auto masterChannelKind = FromProto<EMasterChannelKind>(request->master_channel_kind());

    context->SetRequestInfo("CellTag: %v, MasterChannelKind: %v, RequestCount: %v",
        cellTag,
        masterChannelKind,
        request->part_counts_size());

    if (masterChannelKind != EMasterChannelKind::Leader &&
        masterChannelKind != EMasterChannelKind::Follower)
    {
        THROW_ERROR_EXCEPTION("Expected %Qv or %Qv master channel kind, got %Qv",
            EMasterChannelKind::Leader,
            EMasterChannelKind::Follower,
            masterChannelKind);
    }

    auto session = New<TObjectService::TExecuteSession>(
        MakeStrong(this),
        context,
        cellTag,
        masterChannelKind);
    session->Run();
}

////////////////////////////////////////////////////////////////////////////////

IObjectServicePtr CreateObjectService(IBootstrap* bootstrap)
{
    return New<TObjectService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
