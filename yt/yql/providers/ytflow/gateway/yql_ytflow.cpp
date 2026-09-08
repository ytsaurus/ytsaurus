#include "yql_ytflow.h"
#include "yql_ytflow_config_clusters.h"
#include "yql_ytflow_pipeline_spec.h"
#include "yql_ytflow_prepare.h"
#include "yql_ytflow_logbroker_cm_clients_cache.h"
#include "yql_ytflow_yt_clients_cache.h"
#include "yql_ytflow_worker_config.h"
#include "yql_ytflow_utils.h"

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/misc/guid.h>

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/providers/common/gateway/yql_provider_gateway.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <yt/yql/providers/ytflow/common/yql_ytflow_environment.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_constants.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/scheduler/public.h>
#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/library/arcadia_future_interop/interop.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/flow/library/cpp/pipeline_helpers/pipeline.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/shellcommand.h>
#include <util/system/spinlock.h>
#include <util/system/tempfile.h>


namespace NYql::NYtflow {

using namespace NNodes;

static const TString VANILLA_OPERATION_ID_ATTRIBUTE = "_yql_ytflow_vanilla_operation_id";
static const TString VANILLA_INFO_ATTRIBUTE = "_yql_ytflow_vanilla_info";
static const TString MASTER_LOCK_NODE = "master_lock";

template <typename TValue>
TMaybe<TValue> ConvertOptional(const std::optional<TValue>& value) {
    if (value) {
        return *value;
    }

    return {};
}

DECLARE_REFCOUNTED_STRUCT(TSession);

struct TSession
    : public NYT::TRefCounted
{
    TString Id;
    TOperationProgressWriter OperationProgressWriter;
    TYqlOperationOptions OperationOptions;
    NYT::NConcurrency::IThreadPoolPtr ThreadPool;

    THashMap<TStringBuf, ui32> ComputationCounters;

    NYT::TAtomicIntrusivePtr<NYT::NApi::ITransaction> MasterLockTransaction;
    NYT::TPromise<void> AbortedPromise;

    TSession(const IYtflowGateway::TOpenSessionOptions& options, ui32 threadCount)
        : Id(options.SessionId())
        , OperationProgressWriter(options.OperationProgressWriter())
        , OperationOptions(options.OperationOptions())
        , ThreadPool(NYT::NConcurrency::CreateThreadPool(threadCount, "YtflowGat"))
        , AbortedPromise(NYT::NewPromise<void>())
    { }

    struct TSetProgressOptions
    {
        TMaybe<ui32> PublicId;
        TMaybe<TString> RuntimeCluster;
        TMaybe<TString> OperationId;
        TMaybe<ui64> JobCount;
    };

    void SetProgress(TString status, const TSetProgressOptions& options) {
        if (!options.PublicId) {
            return;
        }

        TOperationProgress progress(
            TString(YtflowProviderName), *options.PublicId,
            TOperationProgress::EState::InProgress);

        if (options.RuntimeCluster && options.OperationId) {
            progress.RemoteId = TStringBuilder()
                << *options.RuntimeCluster
                << "/"
                << *options.OperationId;
        }

        progress.Stage = TOperationProgress::TStage(status, TInstant::Now());

        if (options.JobCount) {
            auto& counters = progress.Counters;
            counters.ConstructInPlace();

            counters->Running = *options.JobCount;
            counters->Total = *options.JobCount;
        }

        OperationProgressWriter(progress);
    }
};

DEFINE_REFCOUNTED_TYPE(TSession);


class TYtflowGateway: public IYtflowGateway {
public:
    TYtflowGateway(const TYtflowServices& services)
        : Services_(services)
        , ConfigClusters_(MakeIntrusive<TConfigClusters>(*Services_.Config))
        , YtClientsCache_(CreateYtClientsCache(ConfigClusters_))
        , LogbrokerCmClientsCache_(CreateLogbrokerCmClientsCache())
        , MoniumClientsCache_(CreateMoniumClientsCache())
    {
    }

    void OpenSession(const TOpenSessionOptions& options) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        YQL_CLOG(DEBUG, ProviderYtflow) << "Open session";

        with_lock (SessionsLock_) {
            if (auto iterator = Sessions_.find(options.SessionId());
                iterator == Sessions_.end()
            ) {
                Sessions_.emplace(std::pair(
                    options.SessionId(),
                    NYT::New<TSession>(options, Services_.Config->GetGatewayThreads())));

                return;
            }

            YQL_ENSURE(false, "Session already exists: " << options.SessionId());
        }
    }

    NThreading::TFuture<void> CloseSession(const TCloseSessionOptions& options) override {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        YQL_CLOG(DEBUG, ProviderYtflow) << "Close session";

        TSessionPtr session;

        with_lock (SessionsLock_) {
            // TODO(ngc224): add assertion after GetUsedClusters elimination
            // (it lacks call to OpenSession)
            if (auto iterator = Sessions_.find(options.SessionId());
                iterator != Sessions_.end()
            ) {
                session = iterator->second;
                Sessions_.erase(iterator);
            }
        }

        NThreading::TFuture<void> future;

        if (session) {
            YQL_CLOG(DEBUG, ProviderYtflow)
                << "Stopping thread pool during session shutdown...";

            session->ThreadPool->Shutdown();

            YQL_CLOG(DEBUG, ProviderYtflow)
                << "Thread pool stopped";

            if (auto tx = session->MasterLockTransaction.Acquire()) {
                future = NYT::ToArcadiaFuture(tx->Abort());

                YQL_CLOG(DEBUG, ProviderYtflow)
                    << "Master lock transaction aborted during session shutdown";
            }
        }

        if (!future.Initialized()) {
            future = NThreading::MakeFuture();
        }

        return future;
    }

    NThreading::TFuture<NCommon::TOperationResult> Run(
        const TExprNode::TPtr& node, const TRunOptions& options, TExprContext& ctx
    ) override {
        try {
            return DoRun(node, options, ctx);
        } catch (const std::exception& ex) {
            auto operationResult = NCommon::TOperationResult();
            operationResult.SetException(ex, ctx.GetPosition(node->Pos()));
            return NThreading::MakeFuture(std::move(operationResult));
        } catch(...) {
            auto operationResult = NCommon::TOperationResult();
            operationResult.SetStatus(EYqlIssueCode::TIssuesIds_EIssueCode_UNEXPECTED);
            operationResult.AddIssue(TIssue({}, CurrentExceptionMessage()));
            return NThreading::MakeFuture(std::move(operationResult));
        }
    }

private:
    TSessionPtr GetSession(const TString& sessionId) {
        with_lock (SessionsLock_) {
            auto iterator = Sessions_.find(sessionId);
            YQL_ENSURE(iterator != Sessions_.end(), "Unknown session: " << sessionId);
            return iterator->second;
        }
    }

    NYT::NApi::IClientPtr GetClient(const TString& cluster, const TString& token) {
        return YtClientsCache_->GetClient(cluster, token);
    }

    NThreading::TFuture<NCommon::TOperationResult> DoRun(
        const TExprNode::TPtr& node, const TRunOptions& options, TExprContext& ctx
    ) {
        if (auto publish = TMaybeNode<TYtflowPublish>(node)) {
            return DoPublish(node, options, ctx);
        } else {
            YQL_ENSURE(false, "Don't know how to execute " << node->Content());
        }
    }

    NThreading::TFuture<NCommon::TOperationResult> DoPublish(
        const TExprNode::TPtr& node, const TRunOptions& options, TExprContext& ctx
    ) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        auto session = GetSession(options.SessionId());
        auto invoker = session->ThreadPool->GetInvoker();

        auto setProgressOptions = TSession::TSetProgressOptions{
            .PublicId = options.PublicId(),
            .RuntimeCluster = options.Config()->GetRuntimeCluster(),
        };

        YQL_CLOG(INFO, ProviderYtflow) << "Preparing pipeline...";
        session->SetProgress("Preparing pipeline", setProgressOptions);

        auto prepareCtx = NPrepare::TContext{
            .ExprContext = ctx,
            .RunOptions = options,
            .ConfigClusters = ConfigClusters_,
        };

        THashMap<TString, TString> secureParams;
        NCommon::FillSecureParams(node, *options.Types(), secureParams);

        auto buildPipelineSpecCtx = TBuildPipelineSpecContext(
            prepareCtx,
            session->ComputationCounters,
            Services_.FunctionRegistry,
            options.UserDataBlocks(),
            secureParams);

        auto [pipelineSpec, requestedCredentials, files] = BuildPipelineSpec(node, buildPipelineSpecCtx);

        auto independentYtPrepareActions = TVector<NPrepare::IActionPtr>{
            NPrepare::CreatePipelineNodeAction(),
            NPrepare::CreateOutputTablesAction()
        };

        for (auto& action : independentYtPrepareActions) {
            action->Init(node, prepareCtx);
        }

        auto dependentYtPrepareActions = TVector<NPrepare::IActionPtr>{
            NPrepare::CreateYtConsumersAction(),
            NPrepare::CreateYtProducersAction()
        };

        for (auto& action : dependentYtPrepareActions) {
            action->Init(node, prepareCtx);
        }

        const auto& ydbToken = requestedCredentials.YdbToken;
        auto createLogbrokerDirectoriesAction = NPrepare::CreateLogbrokerDirectories(
            LogbrokerCmClientsCache_, ydbToken);

        createLogbrokerDirectoriesAction->Init(node, prepareCtx);

        auto logbrokerTopicsAndConsumersActions = TVector{
            NPrepare::CreateInputTopicPermissions(LogbrokerCmClientsCache_, ydbToken),
            NPrepare::CreateOutputLogbrokerTopics(LogbrokerCmClientsCache_, ydbToken),
            NPrepare::CreateLogbrokerConsumers(LogbrokerCmClientsCache_, ydbToken)
        };

        for (auto& action : logbrokerTopicsAndConsumersActions) {
            action->Init(node, prepareCtx);
        }

        auto createLogbrokerReadRulesAction = NPrepare::CreateLogbrokerReadRules(
            LogbrokerCmClientsCache_, ydbToken);

        createLogbrokerReadRulesAction->Init(node, prepareCtx);

        const auto& moniumToken = requestedCredentials.MoniumToken;
        auto checkMoniumProjectsAction = NPrepare::CheckMoniumProjects(
            MoniumClientsCache_, moniumToken);
        auto checkMoniumPermissionsAction = NPrepare::CheckMoniumPermissions(
            MoniumClientsCache_, moniumToken);
        auto createMoniumResourcesAction = NPrepare::CreateMoniumResources(
            MoniumClientsCache_, moniumToken);

        checkMoniumProjectsAction->Init(node, prepareCtx);
        checkMoniumPermissionsAction->Init(node, prepareCtx);
        createMoniumResourcesAction->Init(node, prepareCtx);

        auto publishFuture = AcquireMasterLock(session, *options.Config(), *ConfigClusters_)
            .Apply(BIND([=] {
                TVector<NYT::TFuture<void>> independentYtPrepareFutures;
                for (auto& action : independentYtPrepareActions) {
                    independentYtPrepareFutures.push_back(action->Run(invoker));
                }

                auto ytPrepareFuture = NYT::AllSucceeded(std::move(independentYtPrepareFutures))
                    .Apply(BIND([
                        dependentYtPrepareActions,
                        invoker
                    ]() mutable {
                        TVector<NYT::TFuture<void>> dependentYtPrepareFutures;

                        for (auto& action : dependentYtPrepareActions) {
                            dependentYtPrepareFutures.push_back(action->Run(invoker));
                        }

                        return NYT::AllSucceeded(std::move(dependentYtPrepareFutures));
                    }).AsyncVia(invoker));

                auto createLogbrokerDirectoriesFuture =
                    createLogbrokerDirectoriesAction->Run(invoker);

                auto logbrokerPrepareFuture = createLogbrokerDirectoriesFuture
                    .Apply(BIND([
                        logbrokerTopicsAndConsumersActions,
                        invoker
                    ]() mutable {
                        TVector<NYT::TFuture<void>> logbrokerTopicsAndConsumersFutures;

                        for (auto& action : logbrokerTopicsAndConsumersActions) {
                            logbrokerTopicsAndConsumersFutures.push_back(action->Run(invoker));
                        }

                        return NYT::AllSucceeded(std::move(logbrokerTopicsAndConsumersFutures));
                    }).AsyncVia(invoker))
                    .Apply(BIND([
                        createLogbrokerReadRulesAction,
                        invoker
                    ]() {
                        return createLogbrokerReadRulesAction->Run(invoker);
                    }).AsyncVia(invoker));

                auto moniumPrepareFuture = NYT::OKFuture;
                auto prepareMoniumResources = options.Config()->_MoniumPrepareResources.Get();
                if (prepareMoniumResources && *prepareMoniumResources) {
                    moniumPrepareFuture = checkMoniumProjectsAction->Run(invoker)
                        .Apply(BIND([
                            checkMoniumPermissionsAction,
                            invoker
                        ] {
                            return checkMoniumPermissionsAction->Run(invoker);
                        }).AsyncVia(invoker))
                        .Apply(BIND([
                            createMoniumResourcesAction,
                            invoker
                        ] {
                            return createMoniumResourcesAction->Run(invoker);
                        }).AsyncVia(invoker));
                }

                return NYT::AllSucceeded(TVector{
                    std::move(ytPrepareFuture),
                    std::move(logbrokerPrepareFuture),
                    std::move(moniumPrepareFuture)});
            }).AsyncVia(invoker))
            .Apply(BIND([
                =,
                this,
                this_ = TIntrusivePtr<IYtflowGateway>(this),
                pipelineSpec = std::move(pipelineSpec),
                secureParams = std::move(secureParams),
                // TODO(ngc224): refactor to eliminate need for manual lifetime extension
                requestedCredentials = std::move(requestedCredentials),
                files = std::move(files),
                independentYtPrepareActions = std::move(independentYtPrepareActions),
                dependentYtPrepareActions = std::move(dependentYtPrepareActions),
                createLogbrokerDirectoriesAction = std::move(createLogbrokerDirectoriesAction),
                createLogbrokerTopicsAndConsumersActions = std::move(logbrokerTopicsAndConsumersActions),
                createLogbrokerReadRulesAction = std::move(createLogbrokerReadRulesAction),
                checkMoniumProjectsAction = std::move(checkMoniumProjectsAction),
                checkMoniumPermissionsAction = std::move(checkMoniumPermissionsAction),
                createMoniumResourcesAction = std::move(createMoniumResourcesAction)
            ]() mutable {
                auto result = ExecPublish(
                    std::move(pipelineSpec),
                    options,
                    std::move(secureParams),
                    std::move(requestedCredentials),
                    std::move(files));

                return session->MasterLockTransaction.Acquire()->Commit()
                    .Apply(BIND([=] (const NYT::NApi::TTransactionCommitResult&) {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

                        YQL_CLOG(INFO, ProviderYtflow)
                            << "Master lock transaction committed";

                        return result;
                    }).AsyncVia(invoker));
            }).AsyncVia(invoker));

        auto abortedFuture = session->AbortedPromise.ToFuture();

        auto future = NYT::AnySet(
            std::vector{publishFuture.AsVoid(), abortedFuture},
            NYT::TFutureCombinerOptions{
                .CancelInputOnShortcut = false,
            })
            .Apply(BIND([=] (const NYT::TError&) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

                if (abortedFuture.IsSet()) {
                    YQL_CLOG(INFO, ProviderYtflow)
                        << "Master lock transaction externally aborted";

                    auto error = abortedFuture.GetOrCrash();
                    publishFuture.Cancel(error);

                    return NYT::MakeFuture<NCommon::TOperationResult>(error);
                }

                auto future = publishFuture;
                const auto& publishResult = publishFuture.GetOrCrash();
                if (!publishResult.IsOK()) {
                    YQL_CLOG(ERROR, ProviderYtflow)
                        << "Pipeline publishing failed: "
                        << NYT::ToString(publishResult);
                    if (auto tx = session->MasterLockTransaction.Acquire()) {
                        future = tx->Abort()
                            .Apply(BIND([=] {
                                YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

                                YQL_CLOG(INFO, ProviderYtflow)
                                    << "Master lock transaction aborted";

                                return publishFuture;
                            }).AsyncVia(invoker));
                    }
                }

                return future;
            }).AsyncVia(invoker))
            .Apply(BIND([=] (const NYT::TErrorOr<NCommon::TOperationResult>& valueOr) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

                session->MasterLockTransaction.Reset();

                YQL_CLOG(INFO, ProviderYtflow)
                    << "Master lock transaction dropped";

                return NYT::MakeFuture(valueOr);
            }).AsyncVia(invoker));

        return NYT::ToArcadiaFuture(std::move(future));
    }

    NYT::TFuture<void> AcquireMasterLock(
        TSessionPtr session,
        const TYtflowSettings& config,
        const TConfigClusters& configClusters
    ) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(session->Id);

        auto cluster = config.Cluster.Get();
        YQL_ENSURE(cluster, "Ytflow.Cluster pragma is not set");
        auto pipelineCluster = *cluster;

        auto absolutePipelinePath = NPrivate::GetCanonicalPipelinePath(config);

        auto masterLockPath = NYT::Format(
            "%v/%v/%v",
            absolutePipelinePath,
            YTFLOW_SUBDIRECTORY,
            MASTER_LOCK_NODE);

        auto pipelineToken = ::NYql::NYtflow::NPrivate::GetAuth(pipelineCluster, config, configClusters);
        auto pipelineClient = GetClient(pipelineCluster, pipelineToken);

        auto masterLockTimeout = config._MasterLockTimeout.Get();
        YQL_ENSURE(masterLockTimeout, "Ytflow._MasterLockTimeout system setting is not set");

        auto masterLockPingPeriod = config._MasterLockPingPeriod.Get();
        YQL_ENSURE(masterLockPingPeriod, "Ytflow._MasterLockPingPeriod system setting is not set");

        auto rpcTimeout = config._RpcTimeout.Get();
        YQL_ENSURE(rpcTimeout, "Ytflow._RpcTimeout system setting is not set");

        YQL_CLOG(INFO, ProviderYtflow) << "Starting master lock transaction...";

        NYT::NApi::TCreateNodeOptions createNodeOptions;
        createNodeOptions.Timeout = *rpcTimeout;
        createNodeOptions.Recursive = true;
        createNodeOptions.IgnoreExisting = true;

        auto invoker = session->ThreadPool->GetInvoker();

        return pipelineClient->CreateNode(
            masterLockPath,
            NYT::NObjectClient::EObjectType::Document,
            std::move(createNodeOptions))
            .Apply(BIND([=](const NYT::NCypressClient::TNodeId&) {
                auto attributes = NYT::NYTree::CreateEphemeralAttributes();

                attributes->Set(
                    "title",
                    NPrivate::MakeOperationTitle(session->OperationOptions));

                attributes->Set(
                    "description",
                    NYT::NYTree::ConvertTo<NYT::NYTree::IMapNodePtr>(
                        NYT::NYson::TYsonString{
                            NYT::NodeToYsonString(NPrivate::MakeOperationDescription(
                                session->OperationOptions, config, configClusters))
                        }));

                NYT::NApi::TTransactionStartOptions transactionStartOptions;
                transactionStartOptions.Attributes = std::move(attributes);
                transactionStartOptions.Timeout = *masterLockTimeout;
                transactionStartOptions.PingPeriod = *masterLockPingPeriod;

                return pipelineClient->StartTransaction(
                    NYT::NTransactionClient::ETransactionType::Master,
                    transactionStartOptions)
                    .Apply(BIND([=](const NYT::NApi::ITransactionPtr& tx) {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(session->Id);

                        YQL_CLOG(INFO, ProviderYtflow) << NYT::Format(
                            "Master lock transaction started: %v",
                            tx->GetId());

                        session->MasterLockTransaction.Store(tx);

                        tx->SubscribeAborted(BIND([session](const NYT::TError& error) {
                            session->AbortedPromise.TrySet(error);
                        }).Via(invoker));

                        NYT::NApi::TLockNodeOptions lockOptions;
                        lockOptions.Timeout = *rpcTimeout;
                        lockOptions.Waitable = false;

                        return tx->LockNode(
                            masterLockPath,
                            NYT::NCypressClient::ELockMode::Exclusive,
                            std::move(lockOptions));
                    }).AsyncVia(invoker)).AsVoid();
            }).AsyncVia(invoker));
    }

    NCommon::TOperationResult ExecPublish(
        NYT::NFlow::TPipelineSpecPtr pipelineSpec,
        const TRunOptions& options,
        const THashMap<TString, TString>& secureParams,
        TRequestedCredentials requestedCredentials,
        TVector<TFile> files
    ) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        const auto& config = options.Config();
        auto pipelineCluster = config->Cluster.Get();
        auto runtimeCluster = config->GetRuntimeCluster();

        YQL_ENSURE(pipelineCluster, "Ytflow.Cluster pragma is not set");

        auto pipelineClusterToken = ::NYql::NYtflow::NPrivate::GetAuth(
            *pipelineCluster, *config, *ConfigClusters_);

        auto runtimeClusterToken = ::NYql::NYtflow::NPrivate::GetAuth(
            runtimeCluster, *config, *ConfigClusters_);

        auto pipelineClient = GetClient(*pipelineCluster, pipelineClusterToken);
        auto runtimeClient = GetClient(runtimeCluster, runtimeClusterToken);

        auto session = GetSession(options.SessionId());

        TVector<TTempFileHandle> tempFiles;

        for (auto& file : files) {
            if (file.Disposition == EFileDisposition::InlineData) {
                auto& tempFile = tempFiles.emplace_back();

                auto fileOutput = TFileOutput(tempFile);
                fileOutput.Write(file.Content);

                file.Content = TString(tempFile.Name());
                file.Disposition = EFileDisposition::Path;
            }
        }

        auto workerConfig = MakeWorkerConfig(
            session->OperationOptions,
            *options.Config(),
            *ConfigClusters_,
            options.UserDataBlocks(),
            files);

        const auto& pipelinePath = workerConfig["path"].AsString();

        auto setProgressOptions = TSession::TSetProgressOptions{
            .PublicId = options.PublicId(),
            .RuntimeCluster = runtimeCluster,
        };

        auto rpcTimeout = config->_RpcTimeout.Get();
        YQL_ENSURE(rpcTimeout, "Ytflow._RpcTimeout system setting is not set");

        auto logsDirectory = config->_LogsDirectory.Get();
        YQL_ENSURE(logsDirectory, "Ytflow._LogsDirectory system setting is not set");

        auto stopFuture = StopPreviousRun(
            *pipelineCluster,
            pipelineClient,
            pipelinePath,
            options.Config(),
            session,
            setProgressOptions,
            *rpcTimeout
        );

        NYT::NConcurrency::WaitFor(stopFuture)
            .ThrowOnError();

        // TODO: improve scope restoration as it's not intuitive at all
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        auto secureVault = NYT::TNode::CreateMap()
            // TODO(ngc224): pass actual yt token, not ytflow one
            ("YT_TOKEN", runtimeClusterToken)
            (SecureParamsEnvironmentVariable,
                NYT::NYson::ConvertToYsonString(
                    NYT::NYTree::ConvertToNode(secureParams),
                    NYT::NYson::EYsonFormat::Text).ToString())
            ("YDB_TOKEN", requestedCredentials.YdbToken)
            // Surface the monium token under both env-var names so the worker
            // can read either. MONIUM_TOKEN is the primary (what the gRPC
            // monium driver consults first); SOLOMON_TOKEN is the legacy alias
            // kept for any consumer that still hard-codes the old name.
            ("MONIUM_TOKEN", requestedCredentials.MoniumToken)
            ("SOLOMON_TOKEN", requestedCredentials.MoniumToken);

        auto jobEnvironment = NYT::TNode::CreateMap()
            ("YQL_YTFLOW_LOGS_DIRECTORY", *logsDirectory);

        auto environment = THashMap<TString, TString>{
            {"YQL_YTFLOW_SECURE_VAULT", NYT::NodeToYsonString(secureVault)},
            {"YQL_YTFLOW_JOB_ENVIRONMENT", NYT::NodeToYsonString(jobEnvironment)},
            {"YT_PROXY_URL_ALIASING_CONFIG",
                NYT::NYson::ConvertToYsonString(
                    ConfigClusters_->GetProxyUrlAliasingRules()).ToString()},
            // TODO(ngc224): avoid excessive var passing
            {"YT_TOKEN", runtimeClusterToken}
        };

        YQL_CLOG(INFO, ProviderYtflow)
            << "Setting runtime cluster setting on pipeline node...";

        NYT::NConcurrency::WaitFor(
            SetMeta(
                pipelineClient,
                workerConfig["path"].AsString(),
                runtimeCluster,
                /*operationId*/ {},
                *rpcTimeout))
            .ThrowOnError();

        // TODO: improve scope restoration as it's not intuitive at all
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        auto startWorkersConfig = PatchWorkerConfig(workerConfig, /*runVanilla*/ true);
        auto setupPipelineSpecConfig = PatchWorkerConfig(workerConfig, /*runVanilla*/ false, pipelineSpec);

        if (auto maybeDumpSpecToDirectory = config->_DumpPipelineSpecToDirectory.Get()) {
            auto configs = TVector{
                std::pair(startWorkersConfig, "start_workers_config.yson"),
                std::pair{setupPipelineSpecConfig, "setup_pipeline_spec_config.yson"},
                std::pair{setupPipelineSpecConfig["pipeline_spec"], "pipeline_spec.yson"}
            };

            for (const auto& [config, filename] : configs) {
                auto fileOutput = TFileOutput(*maybeDumpSpecToDirectory + '/' + filename);

                NYT::NodeToYsonStream(
                    config, &fileOutput, NYson::EYsonFormat::Pretty);
            }
        }

        auto runVanillaOperation = config->_RunVanillaOperation.Get();
        YQL_ENSURE(runVanillaOperation, "Ytflow._RunVanillaOperation system setting is not set");

        if (!*runVanillaOperation) {
            auto operationResult = NCommon::TOperationResult();
            operationResult.SetSuccess();

            return operationResult;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "Starting operation...";
        session->SetProgress("Starting operation", setProgressOptions);

        auto outputNode = RunWorker(startWorkersConfig, environment);

        auto operationId = outputNode["operation_id"];
        YQL_ENSURE(operationId.IsString());

        YQL_CLOG(INFO, ProviderYtflow)
            << "Started vanilla operation with id "
            << operationId.AsString()
            << " on cluster " << runtimeCluster;

        YQL_CLOG(INFO, ProviderYtflow)
            << "Setting runtime cluster and operation id settings on pipeline node...";

        NYT::NConcurrency::WaitFor(
            SetMeta(
                pipelineClient,
                workerConfig["path"].AsString(),
                runtimeCluster,
                operationId.AsString(),
                *rpcTimeout))
            .ThrowOnError();

        // TODO: improve scope restoration as it's not intuitive at all
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(options.SessionId());

        setProgressOptions.OperationId = operationId.AsString();
        setProgressOptions.JobCount = workerConfig["controller_count"].AsUint64() +
            workerConfig["worker_count"].AsUint64();

        YQL_CLOG(INFO, ProviderYtflow) << "Starting pipeline...";
        session->SetProgress("Starting pipeline", setProgressOptions);

        RunWorker(setupPipelineSpecConfig, environment);

        YQL_CLOG(INFO, ProviderYtflow) << "Running pipeline...";
        session->SetProgress("Running pipeline", setProgressOptions);

        auto operationResult = NCommon::TOperationResult();
        operationResult.SetSuccess();

        return operationResult;
    }

    NYT::TFuture<void> StopPreviousRun(
        TString pipelineCluster,
        NYT::NApi::IClientPtr pipelineClient,
        TString pipelinePath,
        TYtflowSettings::TConstPtr config,
        TSessionPtr session,
        const TSession::TSetProgressOptions& setProgressOptions,
        TDuration rpcTimeout
    ) {
        NYT::NApi::TGetNodeOptions getNodeOptions;
        getNodeOptions.Attributes = NYT::NYTree::TAttributeFilter{
            VANILLA_OPERATION_ID_ATTRIBUTE,
            VANILLA_INFO_ATTRIBUTE,
        };

        getNodeOptions.Timeout = rpcTimeout;

        YQL_CLOG(INFO, ProviderYtflow)
            << "Getting previous operation info...";

        auto invoker = session->ThreadPool->GetInvoker();

        return pipelineClient->GetNode(
            pipelinePath,
            std::move(getNodeOptions))
            .Apply(BIND([
                =,
                this,
                this_ = TIntrusivePtr<IYtflowGateway>(this),
                logCtx = NYql::NLog::CurrentLogContextPath()
            ] (const NYT::NYson::TYsonString& serializedNode) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                auto node = NYT::NYTree::ConvertTo<NYT::NYTree::INodePtr>(serializedNode);

                TMaybe<TString> previousOperationId;
                TMaybe<TString> previousRuntimeCluster;
                TMaybe<bool> isLegacyAttribute;

                if (auto operationId = node->Attributes()
                    .Find<TString>(VANILLA_OPERATION_ID_ATTRIBUTE)
                ) {
                    previousOperationId = ConvertOptional(operationId);
                    previousRuntimeCluster = pipelineCluster;
                    isLegacyAttribute = true;
                } else if (auto operationInfo = node->Attributes()
                    .Find<NYT::NYTree::IMapNodePtr>(VANILLA_INFO_ATTRIBUTE)
                ) {
                    previousOperationId = ConvertOptional(
                        operationInfo->FindChildValue<TString>("operation_id"));

                    previousRuntimeCluster = ConvertOptional(
                        operationInfo->FindChildValue<TString>("runtime_cluster"));

                    isLegacyAttribute = false;
                }

                auto gracefulUpdate = config->GracefulUpdate.Get();
                YQL_ENSURE(gracefulUpdate, "Ytflow.GracefulUpdate pragma is not set");

                NYT::NFlow::EPipelineState expectedStopState = *gracefulUpdate
                    ? NYT::NFlow::EPipelineState::Stopped
                    : NYT::NFlow::EPipelineState::Paused;

                auto updateTimeout = config->UpdateTimeout.Get();
                YQL_ENSURE(updateTimeout, "Ytflow.UpdateTimeout pragma is not set");

                return FetchPreviousOperations(
                    previousRuntimeCluster,
                    previousOperationId,
                    *config,
                    rpcTimeout,
                    invoker)
                    .Apply(BIND([
                        =,
                        this,
                        this_ = TIntrusivePtr<IYtflowGateway>(this),
                        waitTimeout = *updateTimeout
                    ] (const TVector<NYT::NApi::TOperation>& previousOperations) {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                        if (!previousOperations) {
                            return NYT::OKFuture;
                        }

                        auto token = ::NYql::NYtflow::NPrivate::GetAuth(
                            *previousRuntimeCluster, *config, *ConfigClusters_);

                        auto runtimeClient = GetClient(*previousRuntimeCluster, token);

                        TVector<NYT::NScheduler::TOperationId> aliveOperationIds;
                        for (const auto& operation : previousOperations) {
                            auto operationState = operation.State;
                            YQL_ENSURE(operationState, "Unknown operation state");

                            switch (*operationState) {
                            case NYT::NScheduler::EOperationState::Aborted:
                            case NYT::NScheduler::EOperationState::Failed:
                            case NYT::NScheduler::EOperationState::Completed:
                                break;

                            default:
                                aliveOperationIds.push_back(*operation.Id);
                                break;
                            }
                        }

                        if (!aliveOperationIds) {
                            YQL_CLOG(INFO, ProviderYtflow)
                                << "Found no alive previous operations";

                            return NYT::OKFuture;
                        }

                        YQL_CLOG(INFO, ProviderYtflow) << NYT::Format(
                            "Found alive previous operations on cluster %v: %v",
                            *previousRuntimeCluster,
                            aliveOperationIds);

                        NYT::TFuture<void> stopFuture;

                        if (*gracefulUpdate) {
                            YQL_CLOG(INFO, ProviderYtflow) << "Stopping pipeline...";

                            session->SetProgress("Stopping pipeline", setProgressOptions);

                            NYT::NApi::TStopPipelineOptions stopPipelineOptions;
                            stopPipelineOptions.Timeout = rpcTimeout;

                            stopFuture = pipelineClient->StopPipeline(
                                pipelinePath, std::move(stopPipelineOptions));
                        } else {
                            YQL_CLOG(INFO, ProviderYtflow) << "Pausing pipeline...";

                            session->SetProgress("Pausing pipeline", setProgressOptions);

                            NYT::NApi::TPausePipelineOptions pausePipelineOptions;
                            pausePipelineOptions.Timeout = rpcTimeout;

                            stopFuture = pipelineClient->PausePipeline(
                                pipelinePath, std::move(pausePipelineOptions));
                        }

                        return stopFuture
                            .Apply(BIND([=] {
                                NYT::NFlow::WaitPipelineState(
                                    pipelineClient,
                                    pipelinePath,
                                    expectedStopState,
                                    waitTimeout,
                                    rpcTimeout);
                            }).AsyncVia(invoker))
                            .Apply(BIND([
                                =,
                                this,
                                this_ = TIntrusivePtr<IYtflowGateway>(this)
                            ] {
                                return StopOperations(
                                    aliveOperationIds,
                                    runtimeClient,
                                    rpcTimeout,
                                    logCtx,
                                    invoker,
                                    session,
                                    setProgressOptions);
                            }).AsyncVia(invoker));
                    }).AsyncVia(invoker))
                    .Apply(BIND([=] {
                        if (!isLegacyAttribute.Defined()) {
                            return NYT::OKFuture;
                        }

                        TString attributeName = *isLegacyAttribute
                            ? VANILLA_OPERATION_ID_ATTRIBUTE
                            : VANILLA_INFO_ATTRIBUTE;

                        NYT::NApi::TRemoveNodeOptions removeNodeOptions;
                        removeNodeOptions.Timeout = rpcTimeout;

                        return pipelineClient->RemoveNode(
                            NYT::Format(
                                "%v/@%v",
                                pipelinePath,
                                attributeName),
                            std::move(removeNodeOptions));
                    }).AsyncVia(invoker));
                }).AsyncVia(invoker));
    }

    NYT::TFuture<TVector<NYT::NApi::TOperation>> FetchPreviousOperations(
        TMaybe<TString> previousRuntimeCluster,
        TMaybe<TString> previousOperationId,
        const TYtflowSettings& config,
        TDuration rpcTimeout,
        NYT::IInvokerPtr invoker
    ) {
        if (!previousRuntimeCluster) {
            return NYT::MakeFuture<TVector<NYT::NApi::TOperation>>({});
        }

        YQL_CLOG(INFO, ProviderYtflow)
            << "Fetching previous operations...";

        auto token = ::NYql::NYtflow::NPrivate::GetAuth(
            *previousRuntimeCluster, config, *ConfigClusters_);

        auto runtimeClient = GetClient(*previousRuntimeCluster, token);

        NYT::TFuture<NYT::NApi::TOperation> getOperationFuture;
        if (previousOperationId) {
            auto operationId = NYT::NScheduler::TOperationId(
                NYT::TGuid::FromString(*previousOperationId));

            NYT::NApi::TGetOperationOptions getOperationOptions;
            getOperationOptions.Attributes = {"id", "state"};
            getOperationOptions.Timeout = rpcTimeout;

            getOperationFuture = runtimeClient->GetOperation(
                operationId, std::move(getOperationOptions));
        }

        NYT::NApi::TListOperationsOptions listOperationsOptions;
        listOperationsOptions.IncludeArchive = false;
        listOperationsOptions.IncludeCounters = false;
        listOperationsOptions.SubstrFilter = "yql_pipeline_path";
        listOperationsOptions.TypeFilter = NYT::NScheduler::EOperationType::Vanilla;
        listOperationsOptions.Attributes = {"id", "provided_spec", "state"};
        listOperationsOptions.Timeout = rpcTimeout;

        auto listOperationsFuture = runtimeClient->ListOperations(
            std::move(listOperationsOptions));

        auto futures = TVector<NYT::TFuture<void>>{
            listOperationsFuture.AsVoid()
        };

        if (getOperationFuture) {
            futures.push_back(getOperationFuture.AsVoid());
        }

        return NYT::AllSet(std::move(futures))
            .Apply(BIND([=] (const std::vector<NYT::TError>&) {
                THashSet<NYT::NScheduler::TOperationId> operationIds;
                TVector<NYT::NApi::TOperation> operations;

                if (getOperationFuture) {
                    auto value = getOperationFuture.GetOrCrash();
                    if (!value.IsOK()) {
                        if (value.GetNonTrivialCode() != NYT::NScheduler::EErrorCode::NoSuchOperation) {
                            return NYT::MakeFuture<TVector<NYT::NApi::TOperation>>(
                                static_cast<NYT::TError>(value));
                        }
                    }

                    operations.push_back(value.ValueOrCrash());

                    YQL_ENSURE(operations.back().Id, "Unknown operation id");
                    operationIds.emplace(*operations.back().Id);
                }

                auto listOperationsResultValueOr = listOperationsFuture.GetOrCrash();
                if (!listOperationsResultValueOr.IsOK()) {
                    return NYT::MakeFuture<TVector<NYT::NApi::TOperation>>(
                        static_cast<NYT::TError>(listOperationsResultValueOr));
                }

                auto listOperationsResult = listOperationsResultValueOr.ValueOrCrash();

                // TODO(ngc224): support operation listing
                YQL_ENSURE(!listOperationsResult.Incomplete, "Too many operations");

                for (const auto& listedOperation : listOperationsResult.Operations) {
                    YQL_ENSURE(listedOperation.Id, "Unknown operation id");

                    auto providedSpec = NYT::NYTree::ConvertTo<
                        NYT::NYTree::IMapNodePtr>(listedOperation.ProvidedSpec);

                    auto providedDescription = providedSpec->FindChildValue<
                        NYT::NYTree::IMapNodePtr>("description");

                    YQL_ENSURE(
                        providedDescription,
                        "Unknown operation description: "
                            << NYT::Format("%v", *listedOperation.Id));

                    if (!NPrivate::DoesOperationDescriptionMatchPipeline(
                        *providedDescription,
                        config,
                        *ConfigClusters_))
                    {
                        continue;
                    }

                    auto [_, emplaced] = operationIds.emplace(*listedOperation.Id);
                    if (emplaced) {
                        operations.push_back(listedOperation);
                    }
                }

                return NYT::MakeFuture(std::move(operations));
            }).AsyncVia(invoker));
    }

    NYT::TFuture<void> StopOperations(
        const TVector<NYT::NScheduler::TOperationId>& operationIds,
        NYT::NApi::IClientPtr runtimeClient,
        TDuration rpcTimeout,
        const std::pair<TString, TString>& logCtx,
        NYT::IInvokerPtr invoker,
        TSessionPtr session,
        TSession::TSetProgressOptions setProgressOptions
    ) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

        YQL_CLOG(INFO, ProviderYtflow) << "Stopping operations...";
        session->SetProgress("Stopping operation", setProgressOptions);

        TVector<NYT::TFuture<void>> futures;

        NYT::NApi::TAbortOperationOptions abortOperationOptions;
        abortOperationOptions.Timeout = rpcTimeout;

        for (const auto& operationId : operationIds) {
            auto future = runtimeClient->AbortOperation(
                operationId,
                abortOperationOptions)
                .Apply(BIND([=] (const NYT::TError& value) {
                    if (!value.IsOK() &&
                        value.GetNonTrivialCode() != NYT::NScheduler::EErrorCode::NoSuchOperation
                    ) {
                        return NYT::MakeFuture(value);
                    }

                    return NYT::OKFuture;
                }).AsyncVia(invoker));

            futures.push_back(future);
        }

        return NYT::AllSucceeded(std::move(futures))
            .Apply(BIND([=] {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                YQL_CLOG(INFO, ProviderYtflow) << "Stopped operations";
            }).AsyncVia(invoker));
    }

    NYT::TFuture<void> SetMeta(
        NYT::NApi::IClientPtr pipelineClient,
        TString pipelinePath,
        TString runtimeCluster,
        TMaybe<TString> operationId,
        TDuration rpcTimeout
    ) {
        auto ysonBuilder = NYT::NYTree::BuildYsonStringFluently()
            .BeginMap()
                .Item("runtime_cluster").Value(runtimeCluster);

        if (operationId) {
            ysonBuilder.Item("operation_id").Value(*operationId);
        }

        auto ysonString = ysonBuilder.EndMap();

        NYT::NApi::TSetNodeOptions setNodeOptions;
        setNodeOptions.Timeout = rpcTimeout;

        return pipelineClient->SetNode(
            NYT::Format(
                "%v/@%v",
                pipelinePath,
                VANILLA_INFO_ATTRIBUTE),
            ysonString,
            std::move(setNodeOptions));
    }

    NYT::TNode PatchWorkerConfig(
        const NYT::TNode& workerConfig,
        bool runVanilla,
        std::optional<NYT::NFlow::TPipelineSpecPtr> pipelineSpec = std::nullopt
    ) {
        auto patchedWorkerConfig = workerConfig;
        patchedWorkerConfig
            ("run_vanilla", runVanilla);

        if (pipelineSpec) {
            NYT::TNode pipelineSpecNode;
            NYT::TNodeBuilder nodeBuilder(&pipelineSpecNode);

            (*pipelineSpec)->Save(&nodeBuilder);

            patchedWorkerConfig
                ("pipeline_spec", pipelineSpecNode);
        }

        return patchedWorkerConfig;
    }

    NYT::TNode RunWorker(
        const NYT::TNode& workerConfig,
        const THashMap<TString, TString>& environment
    ) {
        auto configFile = TTempFileHandle();

        {
            auto fileOutput = TFileOutput(configFile);

            NYT::NodeToYsonStream(
                workerConfig, &fileOutput, NYson::EYsonFormat::Pretty);
        }

        auto shellCommandOptions = TShellCommandOptions()
            .SetUseShell(false)
            .SetDetachSession(false);

        shellCommandOptions.Environment = environment;

        auto shellCommand = TShellCommand(
            Services_.Config->GetYtflowWorkerBin(),
            {"--config", configFile.Name()},
            std::move(shellCommandOptions));

        YQL_CLOG(INFO, ProviderYtflow)
            << "Starting command " << shellCommand.GetQuotedCommand()
            << " ...";

        shellCommand.Run();

        auto exitCode = shellCommand.GetExitCode().GetOrElse(0);

        YQL_CLOG(INFO, ProviderYtflow)
            << "Finished command " << shellCommand.GetQuotedCommand()
            << " with exit code " << exitCode;

        YQL_ENSURE(
            !exitCode,
            "Failure during command run, stderr: " << shellCommand.GetError());

        const auto& output = shellCommand.GetOutput();
        if (!output) {
            return NYT::TNode::CreateMap();
        }

        auto outputNode = NYT::NodeFromYsonString(output);
        YQL_ENSURE(outputNode.IsMap());

        return outputNode;
    }

private:
    TYtflowServices Services_;
    TConfigClusters::TPtr ConfigClusters_;
    IYtClientsCachePtr YtClientsCache_;

    ILogbrokerCmClientsCachePtr LogbrokerCmClientsCache_;
    IMoniumClientsCachePtr MoniumClientsCache_;

    THashMap<TString, TSessionPtr> Sessions_;
    TSpinLock SessionsLock_;
};

} // namespace NYql::NYtflow


namespace NYql {

IYtflowGateway::TPtr CreateYtflowGateway(const TYtflowServices& services)
{
    return MakeIntrusive<NYtflow::TYtflowGateway>(services);
}

} // namespace NYql
