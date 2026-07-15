#include "node.h"

#include "admin_http_handlers.h"
#include "admin_service.h"
#include "config.h"
#include "debug_build_warning.h"
#include "endpoint_provider.h"
#include "node_info.h"
#include "private.h"
#include "queue_log_writer.h"

#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>
#include <yt/yt/flow/library/cpp/companion/companion_singleton_state.h>
#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/controller/controller.h>
#include <yt/yt/flow/library/cpp/controller/controller_service.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor.h>
#include <yt/yt/flow/library/cpp/controller/persisted_state_manager.h>
#include <yt/yt/flow/library/cpp/controller/throttler_host.h>
#include <yt/yt/flow/library/cpp/controller/worker_tracker.h>
#include <yt/yt/flow/library/cpp/controller/worker_tracker_service.h>
#include <yt/yt/flow/library/cpp/controller/yt_connector.h>

#include <yt/yt/flow/library/cpp/worker/buffer_state_manager.h>
#include <yt/yt/flow/library/cpp/worker/config.h>
#include <yt/yt/flow/library/cpp/worker/controller_connector.h>
#include <yt/yt/flow/library/cpp/worker/input_manager.h>
#include <yt/yt/flow/library/cpp/worker/job.h>
#include <yt/yt/flow/library/cpp/worker/job_tracker.h>
#include <yt/yt/flow/library/cpp/worker/message_distributor.h>
#include <yt/yt/flow/library/cpp/worker/message_service.h>
#include <yt/yt/flow/library/cpp/worker/public.h>

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/internal_urls.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/yt_connector.h>

#include <yt/yt/flow/library/cpp/misc/backtrace_ypath_service.h>
#include <yt/yt/flow/library/cpp/misc/crash_recorder.h>
#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/lib/native_client/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/orchid/orchid_service.h>
#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>
#include <yt/yt/library/profiling/solomon/proxy.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/https/client.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/mlock/mlock.h>
#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <util/string/split.h>

#include <cstdlib>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NCoreDump;
using namespace NLogging;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NTracing;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NodeLogger;

constexpr auto& JaegerCollectorAddressSuffix = NInternalUrls::JaegerCollectorAddressSuffix;

////////////////////////////////////////////////////////////////////////////////

class TFlowNodeProgram final
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TFlowNodeConfig>
{
public:
    TFlowNodeProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    void DoRun() override
    {
        try {
            DoRunNode();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_ERROR(error, "Flow node failed");
            THROW_ERROR_EXCEPTION("Flow node failed") << error;
        }
    }

private:
    EFlowRunMode Mode_ = {};
    TFlowNodeConfigPtr Config_;
    NYTree::INodePtr ConfigNode_;
    TNodeInfoPtr NodeInfo_;

    TRichYPath PipelinePath_;
    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NYTree::IMapNodePtr OrchidRoot_;
    IStatusProfilerPtr RootStatusProfiler_;
    NConcurrency::IEnumIndexedFairShareActionQueuePtr<NController::EControlQueue> ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NConcurrency::IPollerPtr HttpPoller_;
    NHttp::IServerPtr HttpServer_;
    NHttp::IClientPtr HttpClient_;
    NHttp::IClientPtr HttpsClient_;

    NProfiling::TSolomonProxyPtr SolomonProxy_;

    IPipelineAuthenticatorPtr PipelineAuthenticator_;

    ICommonYTConnectorPtr CommonYTConnector_;

    NController::IYTConnectorPtr ControllerYTConnector_;
    NController::IWorkerTrackerPtr WorkerTracker_;
    NConcurrency::IFairShareThreadPoolPtr ControllerThreadPool_;
    NController::IPersistedStateManagerPtr PersistedStateManager_;
    NController::IThrottlerHostPtr ThrottlerHost_;
    NController::IControllerPtr Controller_;
    NController::IFlowExecutorPtr FlowExecutor_;

    NRpc::IChannelFactoryPtr ChannelFactory_;
    IJobDirectoryPtr JobDirectory_;
    NWorker::IInputManagerPtr InputManager_;
    NConcurrency::IThreadPoolPtr MessageServiceThreadPool_;
    NWorker::IMessageDistributorPtr MessageDistributor_;
    NWorker::IJobTrackerPtr JobTracker_;
    NWorker::IControllerConnectorPtr ControllerConnector_;

private:
    void DoRunNode()
    {
        // Unconditional configuring.
        ::TThread::SetCurrentThreadName("FlowMain");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        ConfigureAllocator({
            .SnapshotUpdatePeriod = TDuration::Seconds(5),
        });
        MlockFileMappings();
        RunMixinCallbacks();

        TErrorCodicils::Initialize();
        RegisterQueueLogWriterFactory();

        // Config-dependent configuring.
        Mode_ = ParseMode();

        auto config = GetConfigNode();
        if (const char* overridesEnvValue = std::getenv("YT_FLOW_CONFIG")) {
            config = PatchNode(config, ConvertToNode(NYson::TYsonString(TStringBuf(overridesEnvValue))));
        }
        Config_ = ConvertTo<TFlowNodeConfigPtr>(config);
        // Ports come from the config by default. When the operation requests YT-allocated
        // ports (port_count > 0, e.g. on a shared-network host), YT exposes them via
        // YT_PORT_<i> — honor those over the config: YT_PORT_0 → rpc_port (and bus_server.port),
        // YT_PORT_1 → monitoring_port, YT_PORT_2 → companion.port (python/java workers only).
        if (const char* port0Env = std::getenv("YT_PORT_0")) {
            int rpcPort = FromString<int>(port0Env);
            Config_->RpcPort = rpcPort;
            Config_->BusServer->Port = rpcPort;
        }
        if (const char* port1Env = std::getenv("YT_PORT_1")) {
            Config_->MonitoringPort = FromString<int>(port1Env);
        }
        if (const char* port2Env = std::getenv("YT_PORT_2")) {
            // Companion is optional in YSON, so create it on demand.
            if (!Config_->Companion) {
                Config_->Companion = New<NCompanion::TCompanionConfig>();
            }
            Config_->Companion->Port = FromString<int>(port2Env);
        }
        ConfigNode_ = ConvertToNode(Config_);

        SetupCrashRecording(Format("port%v", Config_->RpcPort));

        if (Config_->EnablePhdrCache) {
            EnablePhdrCache();
        }

        PipelinePath_ = Config_->Path;
        PipelinePath_.SetCluster(Config_->ClusterUrl);

        PatchLogManagerConfig(Config_, PipelinePath_, Mode_);

        ConfigureSingletons(Config_); // Notice: will be configured again later.

        MaybeLogSlowBuild(Logger());

        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }

        NodeInfo_ = GetNodeInfo(Config_, Logger());
        NNet::SetLocalHostName(NodeInfo_->Name);

        if (NodeInfo_->VcpuFactor.has_value()) {
            NProfiling::TResourceTracker::SetCpuToVCpuFactor(*NodeInfo_->VcpuFactor);
        }

        ControlQueue_ = NConcurrency::CreateEnumIndexedFairShareActionQueue<NController::EControlQueue>("Control");

        // Switch to running in fiber.
        WaitFor(
            BIND(&TFlowNodeProgram::DoRunNodeInFiber, MakeWeak(this))
                .AsyncVia(ControlQueue_->GetInvoker(NController::EControlQueue::Default))
                .Run())
            .ThrowOnError();

        Sleep(TDuration::Max());
    }

    void DoRunNodeInFiber()
    {
        Prepare();

        if (Any(Mode_ & EFlowRunMode::Worker)) {
            PrepareWorker();
        }
        if (Any(Mode_ & EFlowRunMode::Controller)) {
            PrepareController();
        }

        YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
        RpcServer_->Configure(Config_->RpcServer);
        RpcServer_->Start();

        // Start the connector after all preparations to guarantee that
        // SubscribeLeadingStarted callbacks are initialized before the Controller election.
        if (ControllerYTConnector_) {
            YT_LOG_INFO("Starting Controller-to-YT connector");
            ControllerYTConnector_->Start();
        }
    }

    void Prepare()
    {
        BusServer_ = NBus::NTcp::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpPoller_ = CreateThreadPoolPoller(Config_->HttpPollerThreads, "HttpPoller");
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig(), HttpPoller_);
        HttpClient_ = NHttp::CreateClient(Config_->HttpClientConfig, HttpPoller_);
        HttpsClient_ = NHttps::CreateClient(Config_->HttpsClientConfig, HttpPoller_);

        Config_->SolomonExporter->InstanceTags["pipeline_path"] = Config_->Path;
        Config_->SolomonExporter->InstanceTags["pipeline_cluster"] = Config_->ClusterUrl;
        NMonitoring::Initialize(
            HttpServer_,
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        // Combined sensors endpoint (this node + embedded companion, when present).
        // Uses a dedicated prefix because the exporter already owns "/solomon/sensors".
        SolomonProxy_ = New<NProfiling::TSolomonProxy>(Config_->SolomonProxy, HttpPoller_);
        SolomonProxy_->Register("/solomon_proxy", HttpServer_);
        SolomonProxy_->RegisterEndpointProvider(New<TFlowEndpointProvider>(
            Config_->MonitoringPort,
            Config_->Companion ? Config_->Companion->MonitoringPort : 0));

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            CreateVirtualNode(ConfigNode_));

        SetNodeByYPath(
            OrchidRoot_,
            "/config_schema",
            CreateVirtualNode(IYPathService::FromProducer(BIND([config = Config_] (IYsonConsumer* consumer) {
                config->WriteSchema(consumer);
            }))));

        SetNodeByYPath(
            OrchidRoot_,
            "/node_info",
            CreateVirtualNode(ConvertToNode(NodeInfo_)));

        SetNodeByYPath(
            OrchidRoot_,
            "/backtraces",
            CreateVirtualNode(CreateBacktraceYPathService()));

        if (Config_->Companion) {
            ForceYPath(OrchidRoot_, "/companion/jfr");
            SetNodeByYPath(
                OrchidRoot_,
                "/companion/jfr",
                CreateVirtualNode(
                    IYPathService::FromProducer(
                        BIND([port = Config_->Companion->Port] (IYsonConsumer* consumer) {
                            auto proxy = NCompanion::CreateCompanionProxy(Format("localhost:%v", port));
                            auto req = proxy.GetJfr();
                            auto rsp = WaitFor(req->Invoke()).ValueOrThrow();

                            if (rsp->status() == NProto::NCompanion::RS_OK && rsp->has_jfr_data()) {
                                // clang-format off
                                BuildYsonFluently(consumer)
                                    .BeginMap()
                                        .Item("data").Value(TStringBuf(rsp->jfr_data()))
                                    .EndMap();
                                // clang-format on
                            } else {
                                auto errorMessage = rsp->has_error_message()
                                    ? rsp->error_message()
                                    : "Unknown error";
                                // clang-format off
                                BuildYsonFluently(consumer)
                                    .BeginMap()
                                        .Item("error").Value(errorMessage)
                                    .EndMap();
                                // clang-format on
                            }
                        }))));
        }

        SetBuildAttributes(
            OrchidRoot_,
            "flow_node");

        RegisterAdminHttpHandlers(HttpServer_);

        auto clientsCacheConfig = CloneYsonStruct(Config_->ClientsCache);
        if (!clientsCacheConfig->DefaultConnection->ProxyRole.has_value()) {
            clientsCacheConfig->DefaultConnection->ProxyRole = Config_->ProxyRole;
        }

        PipelineAuthenticator_ = CreatePipelineAuthenticator(
            PipelinePath_,
            Config_->ProxyRole,
            Config_->Tvm,
            HttpsClient_,
            Config_->Authenticator,
            NodeInfo_,
            clientsCacheConfig);

        RpcServer_->RegisterService(CreateAdminService(
            ControlQueue_->GetInvoker(NController::EControlQueue::Default),
            PipelineAuthenticator_->CreateSelfRpcAuthenticator()));
        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            ControlQueue_->GetInvoker(NController::EControlQueue::Default),
            PipelineAuthenticator_->CreateSelfRpcAuthenticator()));

        auto clientsCache = NClient::NCache::CreateClientsCache(clientsCacheConfig, PipelineAuthenticator_->GetClientOptions());
        CommonYTConnector_ = CreateCommonYTConnector(clientsCache, PipelinePath_);
        SetQueueLogWriterClient(CommonYTConnector_->GetClient());

        // Singletons full reconfiguring.
        auto patchedSingletonsConfig = CloneYsonStruct<TSingletonsConfig>(Config_);
        if (auto tracerConfig = GetTracerConfig(Config_, PipelineAuthenticator_, CommonYTConnector_)) {
            patchedSingletonsConfig->SetSingletonConfig(tracerConfig);
        }
        TSingletonManager::Configure(patchedSingletonsConfig);

        // A combined controller+worker node shares one status profiler; its components report
        // under the controller namespace. The general dashboard sums both roles, so the
        // problematic-places total stays correct regardless of the namespace picked here.
        RootStatusProfiler_ = CreateStatusProfiler(
            ControlQueue_->GetInvoker(NController::EControlQueue::Default),
            Logger(),
            {},
            NProfiling::TProfiler(
                /*prefix*/ "",
                Any(Mode_ & EFlowRunMode::Controller) ? "yt.flow.controller" : "yt.flow.worker"));

        SetNodeByYPath(
            OrchidRoot_,
            "/status_profiler",
            CreateVirtualNode(
                IYPathService::FromProducer(BIND([profiler = RootStatusProfiler_] (IYsonConsumer* consumer) {
                    auto status = profiler->GetStatus();
                    // clang-format off
                    BuildYsonFluently(consumer)
                        .BeginMap()
                            .Item("errors").Value(status.Errors)
                        .EndMap();
                    // clang-format on
                }))));
    }

    void PrepareWorker()
    {
        ChannelFactory_ = NRpc::NBus::CreateTcpBusChannelFactory(Config_->Worker->Bus);

        const auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());

        const auto streamSpecStorage = New<TStreamSpecStorage>(converterCache);

        JobDirectory_ = CreateJobDirectory(converterCache, NWorker::WorkerLogger());

        InputManager_ = NWorker::CreateInputManager();

        PrepareCompanion();

        MessageDistributor_ = NWorker::CreateMessageDistributor(
            JobDirectory_,
            PipelineAuthenticator_->CreateSelfCredentialsInjectingChannelFactory(ChannelFactory_),
            streamSpecStorage);

        auto workerGroups = GetWorkerGroups();
        auto workerCapabilities = GetWorkerCapabilities();

        auto jobTrackerContext = New<NWorker::TJobTrackerContext>();
        jobTrackerContext->WorkerNodeInfo = NodeInfo_;
        jobTrackerContext->ClientsCache = CommonYTConnector_->GetClientsCache();
        jobTrackerContext->PipelinePath = Config_->Path;
        jobTrackerContext->PipelinePath.SetCluster(Config_->ClusterUrl);
        jobTrackerContext->ControlInvoker = ControlQueue_->GetInvoker(NController::EControlQueue::Default);
        jobTrackerContext->MessageDistributor = MessageDistributor_;
        jobTrackerContext->InputManager = InputManager_;
        jobTrackerContext->PipelineAuthenticator = PipelineAuthenticator_;
        jobTrackerContext->StreamSpecStorage = streamSpecStorage;
        jobTrackerContext->JobDirectory = JobDirectory_;
        jobTrackerContext->ClockClusterTag = GetFlowTablesCellTag();
        jobTrackerContext->HttpClient = HttpClient_;
        jobTrackerContext->HttpsClient = HttpsClient_;
        jobTrackerContext->Poller = HttpPoller_;
        jobTrackerContext->StatusProfiler = RootStatusProfiler_;
        // ControllerConnector_ is created just below; at call time it is always
        // initialized (jobs start running only after worker setup is complete).
        jobTrackerContext->DistributedThrottlerChannel = [this] () -> NRpc::IChannelPtr {
            return ControllerConnector_ ? ControllerConnector_->GetControllerChannel() : nullptr;
        };
        JobTracker_ = NWorker::CreateJobTracker(jobTrackerContext);

        ControllerConnector_ = NWorker::CreateControllerConnector(
            NodeInfo_,
            workerGroups,
            workerCapabilities,
            CommonYTConnector_,
            ControlQueue_->GetInvoker(NController::EControlQueue::Default),
            PipelineAuthenticator_->CreateSelfCredentialsInjectingChannelFactory(ChannelFactory_),
            MessageDistributor_,
            JobDirectory_,
            JobTracker_,
            streamSpecStorage,
            Config_->IgnoreSingletonsDynamicConfig,
            RootStatusProfiler_);
        ControllerConnector_->Initialize();

        SetNodeByYPath(
            OrchidRoot_,
            "/worker",
            CreateVirtualNode(ControllerConnector_->CreateOrchidService()->Via(ControlQueue_->GetInvoker(NController::EControlQueue::StaticOrchid))));

        SetNodeByYPath(
            OrchidRoot_,
            "/job_tracker",
            CreateVirtualNode(JobTracker_->CreateOrchidService()->Via(ControlQueue_->GetInvoker(NController::EControlQueue::StaticOrchid))));

        MessageServiceThreadPool_ = NConcurrency::CreateThreadPool(Config_->Worker->MessageServiceThreads, "MsgService");

        RpcServer_->RegisterService(NWorker::CreateMessageService(
            InputManager_,
            PipelineAuthenticator_->CreateSelfRpcAuthenticator(),
            streamSpecStorage,
            MessageServiceThreadPool_->GetInvoker()));
    }

    void PrepareController()
    {
        ChannelFactory_ = NRpc::NBus::CreateTcpBusChannelFactory(Config_->Controller->Bus);
        ControllerYTConnector_ = CreateYTConnector(Config_->Controller, NodeInfo_, CommonYTConnector_, ControlQueue_);

        WorkerTracker_ = CreateWorkerTracker(ControlQueue_, ControllerYTConnector_, NodeInfo_);
        ControllerThreadPool_ = NConcurrency::CreateFairShareThreadPool(Config_->Controller->ControllerThreads, "Controller");
        PersistedStateManager_ = NController::CreatePersistedStateManager(ControllerYTConnector_, Config_->Controller->PersistedStateManager);
        ThrottlerHost_ = NController::CreateThrottlerHost(
            ControllerThreadPool_->GetInvoker("DistributedThrottler"),
            ControllerYTConnector_->GetPipelinePath());
        Controller_ = CreateController(
            Config_->Controller,
            NodeInfo_,
            WorkerTracker_,
            ThrottlerHost_,
            ControllerThreadPool_->GetInvoker("Scheduler"),
            ControllerYTConnector_,
            PersistedStateManager_,
            PipelineAuthenticator_,
            Config_->IgnoreSingletonsDynamicConfig,
            GetFlowTablesCellTag(),
            RootStatusProfiler_);
        FlowExecutor_ = CreateFlowExecutor(
            Controller_,
            PersistedStateManager_,
            ControllerYTConnector_,
            Config_->Controller->ControllerService,
            OrchidRoot_,
            RootStatusProfiler_,
            ControllerThreadPool_->GetInvoker("FlowExecutor"),
            HttpClient_,
            ChannelFactory_,
            PipelineAuthenticator_);
        WorkerTracker_->Initialize();
        Controller_->Initialize();

        RpcServer_->RegisterService(CreateWorkerTrackerService(
            Controller_,
            WorkerTracker_,
            ControllerThreadPool_->GetInvoker("WorkerTrackerService"),
            ChannelFactory_,
            PipelineAuthenticator_));
        RpcServer_->RegisterService(CreateControllerService(
            FlowExecutor_,
            PipelineAuthenticator_,
            ControlQueue_->GetInvoker(NController::EControlQueue::Admin)));
        RpcServer_->RegisterService(ThrottlerHost_->GetRpcService());
    }

    //! Publishes companion parameters into the companion singleton state.
    //! Must be called before CreateJobTracker().
    void PrepareCompanion()
    {
        if (Config_->Companion) {
            NCompanion::SetCompanionExecutionConfig(
                NCompanion::BuildCompanionExecutionConfig(
                    Config_->Companion,
                    Config_->ClusterUrl,
                    Config_->Path));
        }
    }

    EFlowRunMode ParseMode()
    {
        auto rawModeStr = TStringBuf(std::getenv(FlowModeEnvVarName.data()));
        try {
            // Modes are typically separated with "plus" rather than "pipe".
            // Also, let's make parsing case-insensitive.
            std::string modeStr;
            for (auto ch : rawModeStr) {
                if (ch == '+') {
                    ch = '|';
                }
                modeStr.push_back(tolower(ch));
            }
            return ParseEnum<EFlowRunMode>(modeStr);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing %Qv variable",
                    FlowModeEnvVarName)
                << ex;
        }
    }

    std::vector<std::string> GetWorkerGroups() const
    {
        const char* envName = "YT_FLOW_WORKER_GROUPS";
        TStringBuf envValue = std::getenv(envName);
        std::vector<std::string> groups;
        StringSplitter(envValue).Split(',').SkipEmpty().Collect(&groups);
        return groups;
    }

    THashMap<std::string, ssize_t> GetWorkerCapabilities() const
    {
        const char* envName = "YT_FLOW_WORKER_CAPABILITIES";
        TStringBuf envValue = std::getenv(envName);
        THashMap<std::string, ssize_t> capabilities;

        std::vector<TStringBuf> pairs;
        StringSplitter(envValue).Split(',').SkipEmpty().Collect(&pairs);

        for (const auto& pair : pairs) {
            std::vector<TStringBuf> keyValue;
            StringSplitter(pair).SplitBySet(" \t:=").SkipEmpty().Collect(&keyValue);
            if (keyValue.size() == 2) {
                try {
                    auto value = FromString<ssize_t>(keyValue[1]);
                    capabilities[std::string(keyValue[0])] = value;
                } catch (...) {
                    YT_LOG_FATAL(TError(std::string(CurrentExceptionMessage()), TError::DisableFormat),
                        "Failed to parse worker capability value "
                        "(EnvVar: %v, Pair: %v, Key: %v, Value: %v)",
                        envName,
                        pair,
                        keyValue[0],
                        keyValue[1]);
                }
            } else if (keyValue.size() == 1) {
                // Treat "key1,key2,key3" as "key1:0,key2:0,key3:0".
                capabilities[std::string(keyValue[0])] = 0;
            } else {
                YT_LOG_FATAL("Malformed worker capability entry "
                    "(EnvVar: %v, Pair: %v)",
                    envName,
                    pair);
            }
        }

        return capabilities;
    }

    NObjectClient::TCellTag GetFlowTablesCellTag()
    {
        TExponentialBackoffOptions backoffOptions;
        backoffOptions.InvocationCount = std::numeric_limits<decltype(backoffOptions.InvocationCount)>::max();
        backoffOptions.MaxBackoff = TDuration::Minutes(5);
        TBackoffStrategy backoffStrategy(backoffOptions);

        while (true) {
            try {
                auto bundleInfo = WaitFor(CommonYTConnector_->GetFlowTablesBundle()).ValueOrThrow();
                YT_LOG_INFO("Found flow tables bundle clock cluster tag (Bundle: %v, ClockClusterTag: %v)",
                    bundleInfo.Bundle,
                    bundleInfo.ClockClusterTag);
                return bundleInfo.ClockClusterTag.value_or(NObjectClient::InvalidCellTag);
            } catch (const TErrorException& ex) {
                backoffStrategy.Next();
                YT_LOG_ERROR(ex, "Unable to confirm bundles clock cell tag (Attempt: %v, RetryAfter: %v)", backoffStrategy.GetInvocationIndex() - 1, backoffStrategy.GetBackoff());
                TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
            }
        }
    }

    static TPipelineAttributes GetPipelineAttributes(ICommonYTConnectorPtr ytConnector)
    {
        TExponentialBackoffOptions backoffOptions;
        backoffOptions.InvocationCount = std::numeric_limits<decltype(backoffOptions.InvocationCount)>::max();
        backoffOptions.MaxBackoff = TDuration::Minutes(5);
        TBackoffStrategy backoffStrategy(backoffOptions);

        while (true) {
            try {
                return WaitFor(ytConnector->GetPipelineAttributes()).ValueOrThrow();
            } catch (const TErrorException& ex) {
                backoffStrategy.Next();
                YT_LOG_ERROR(ex, "Unable to get pipeline attributes (Attempt: %v, RetryAfter: %v)", backoffStrategy.GetInvocationIndex() - 1, backoffStrategy.GetBackoff());
                TDelayedExecutor::WaitForDuration(backoffStrategy.GetBackoff());
            }
        }
    }

    TJaegerTracerConfigPtr GetTracerConfig(TFlowNodeConfigPtr config, IPipelineAuthenticatorPtr authenticator, ICommonYTConnectorPtr ytConnector)
    {
        if (!authenticator->GetTvmService()) {
            YT_LOG_INFO("Tracing can not be automatically configured, because TVM is not configured");
            return nullptr; // If tvm is not configured, tracer can not be initialized.
        }

        auto pipelineYTAttributes = GetPipelineAttributes(ytConnector);

        auto tracerConfig = CloneYsonStruct(config->GetSingletonConfig<TJaegerTracerConfig>());

        YT_VERIFY(!tracerConfig->TvmService, "Must be nullptr. It is checked in TFlowNodeConfig postprocessor");

        tracerConfig->TvmService = CloneYsonStruct(config->Tvm);
        if (!tracerConfig->ServiceName.has_value() && !pipelineYTAttributes.MonitoringCluster.empty()) {
            tracerConfig->ServiceName = pipelineYTAttributes.MonitoringCluster;
        }
        if (!tracerConfig->CollectorChannelConfig) {
            tracerConfig->CollectorChannelConfig = New<NRpc::NGrpc::TChannelConfig>();
        }
        if (tracerConfig->CollectorChannelConfig->Address.empty() && !JaegerCollectorAddressSuffix.empty() && !pipelineYTAttributes.MonitoringProject.empty()) {
            tracerConfig->CollectorChannelConfig->Address = Format("%v%v", pipelineYTAttributes.MonitoringProject, JaegerCollectorAddressSuffix);
        }
        return tracerConfig;
    }

    static void PatchLogManagerConfig(TFlowNodeConfigPtr config, NYPath::TRichYPath pipelinePath, EFlowRunMode mode)
    {
        if (!Any(mode & EFlowRunMode::Controller)) {
            return;
        }
        static const std::string logWriterName = "ControllerLogWriter";
        auto logManagerConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
        EmplaceOrCrash(logManagerConfig->Writers, logWriterName, GetQueueLogWriterConfig(pipelinePath));

        auto ruleConfig = New<NLogging::TRuleConfig>();
        ruleConfig->IncludeCategories = THashSet<std::string>({
            "PublicFlowController",
        });
        ruleConfig->Writers = {
            TString(logWriterName),
        };
        ruleConfig->MinLevel = NLogging::ELogLevel::Info;
        logManagerConfig->Rules.push_back(ruleConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

int RunFlowNode(int argc, const char** argv)
{
    return New<TFlowNodeProgram>()->Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
