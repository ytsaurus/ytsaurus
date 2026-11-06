#include "worker_dq.h"

#include <yql/essentials/utils/log/log.h>

#include <yql/essentials/utils/yql_panic.h>

#include <yql/essentials/providers/common/metrics/metrics_registry.h>

#include <yql/providers/dq/metrics/metrics_pusher.h>
#include <yt/yql/providers/dq/common/yql_dq_clique.h>
#include <yt/yql/providers/dq/actors/yt/resource_manager.h>
#include <yt/yql/providers/dq/stats_collector/pool_stats_collector.h>
#include <yt/yql/providers/dq/actors/dynamic_nameserver.h>
#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <yt/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>
#include <yt/yql/providers/dq/service/interconnect_helpers.h>
#include <yt/yql/providers/dq/service/service_node.h>
#include <yql/tools/yqlworker/solomon_agent/solomon_agent_client.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>

#include <util/generic/algorithm.h>
#include <util/stream/file.h>

namespace NYql {

using namespace NActors;

void YtReopenLog();
void YtInitLogs(const TString& logFile);
void YtShutdown();

namespace {
    void ReopenLogIfNeeded(TMainConfig& config) {
        if (!NeedReopenLog) {
            return;
        }

        NeedReopenLog = 0;
        NLog::ReopenLog();
        config.ReopenStdErr();
        YtReopenLog();
    }
}

class TWorkerDq::TImpl
{
    TBindResultVector ServerPorts;
    TMainConfig Config;

public:
    TImpl(const TMainConfig& config, const TBindResultVector& serverPorts)
        : ServerPorts(serverPorts)
        , Config(config)
    {
        YQL_ENSURE(ServerPorts.size() >= 2);
    }

    ~TImpl()
    {
    }

    int Run() {
        THolder<TServiceNode> serviceNode;

        int exitCode = 0;
        try {

            auto& dqConfig = Config.Worker.GetDq();

            if (dqConfig.HasYtCoordinator() && dqConfig.GetYtCoordinator().HasDebugLogFile()) {
                YtInitLogs(dqConfig.GetYtCoordinator().GetDebugLogFile());
            }

            auto threads = dqConfig.GetActorThreads();

            TString hostName;
            TString localAddress;

            auto coordinatorConfig = dqConfig.GetYtCoordinator();

            Y_ABORT_UNLESS(ServerPorts.size() >= 4);
            Y_ABORT_UNLESS(ServerPorts[1].Addr.IsIpv6());
            Y_ABORT_UNLESS(ServerPorts[3].Addr.IsIpv6());

            // 0 -- ipv4
            // 1 -- ipv6
            ui16 interconnectPort = ServerPorts[1].Addr.GetPort();
            ui16 grpcPort = ServerPorts[3].Addr.GetPort();
            bool localhostMode = false;

            YQL_ENSURE(interconnectPort != grpcPort);

            if (coordinatorConfig.HasHostName() && coordinatorConfig.GetHostName() == "localhost") {
                // don't use yt for service node resolving (tests startup optimization)
                *coordinatorConfig.AddServiceNodeHostPort() = TString("localhost:") + ToString(grpcPort);
                localhostMode = true;
            }

            std::tie(hostName, localAddress) = NYql::NDqs::GetLocalAddress(
                coordinatorConfig.HasHostName() ? &coordinatorConfig.GetHostName() : nullptr
            );

            if (!coordinatorConfig.HasProxyAddress()) {
                *coordinatorConfig.MutableProxyAddress() = coordinatorConfig.GetClusterName();
            }

            YQL_ENSURE(dqConfig.GetYtBackends().size() > 0);

            const ui32 communalBackendCount = CountIf(
                dqConfig.GetYtBackends(),
                IsCommunalYtBackend);
            const bool globalWorkerManagerStarted = communalBackendCount > 0;

            auto coordinator = CreateCoordiantionHelper(coordinatorConfig, dqConfig.GetScheduler(), "service_node", interconnectPort, hostName, localAddress);

            TMaybe<ui32> maybeNodeId;
            if (dqConfig.HasNodeId()) {
                maybeNodeId = dqConfig.GetNodeId();
            }
            TMaybe<TString> grpcPortForDiscovery;
            if (globalWorkerManagerStarted) {
                grpcPortForDiscovery = ToString(grpcPort);
            }
            auto nodeId = coordinator->GetNodeId(
                maybeNodeId,
                grpcPortForDiscovery,
                static_cast<ui32>(NDqs::ENodeIdLimits::MinServiceNodeId),
                static_cast<ui32>(NDqs::ENodeIdLimits::MaxServiceNodeId),
                {}
            );

            try {
                TFile grpcPortFile("/var/tmp/dq_grpc_port", CreateAlways | WrOnly);
                TString data = ToString(grpcPort);
                grpcPortFile.Write(&data[0], data.length());
            } catch (...) { }

            NDqs::TServiceNodeConfig config = {
                nodeId,
                localAddress,
                hostName,
                interconnectPort,
                grpcPort,
                /*MbusPort=*/0, // TODO: remove, MbusPort not used
                ServerPorts[1].Socket->Release(), // [1] for ipv6
                ServerPorts[3].Socket->Release(), // [1] for ipv6
                Nothing(),
                dqConfig.GetICSettings(),
                [](const TIntrusivePtr<NActors::TTableNameserverSetup>& setup) {
                    return NYql::NDqs::CreateDynamicNameserver(setup);
                },
                {}, // WorkerManagerActorId: GWM registered under the legacy id on this node
            };

            YQL_CLOG(INFO, ProviderDq) << "Interconnect addr/port " << PrintHostAndPort(ServerPorts[1].Addr);
            YQL_CLOG(INFO, ProviderDq) << "GRPC addr/port " << PrintHostAndPort(ServerPorts[3].Addr);

            auto metricsRegistry = CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq));
            serviceNode = MakeHolder<TServiceNode>(config, threads, metricsRegistry);

            auto statsCollector = CreateStatsCollector(5, *serviceNode->GetSetup(), metricsRegistry->GetSensors());
            auto* actorSystem = serviceNode->StartActorSystem();

            actorSystem->Register(statsCollector);

            // push metrics from root group
            TActorId metricsPusherId;
            if (auto config = GetSolomonAgentConfig(Config.Worker)) {
                metricsPusherId = actorSystem->Register(
                    CreateMetricsPusher(GetSensorsRootGroup(), ParseSolomonAgentConfig(*config), /*pidLabel=*/true));
            }

            if (!dqConfig.HasNodeId()) {
                // Keep the actor node id reserved, but publish a gRPC endpoint only
                // when the process actually starts the global worker manager.
                coordinator->StartRegistrator(actorSystem);

                if (!(dqConfig.HasDisableNodeCleaner() && dqConfig.GetDisableNodeCleaner())) {
                    coordinator->StartCleaner(actorSystem, {});
                }
            } else {
                coordinator->GetWrapper(actorSystem);
            }

            auto funcRegistry = CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, TVector<TString>());

            TDqTaskPreprocessorFactoryCollection dqTaskPreprocessorFactories = {
                NDq::CreateYtDqTaskPreprocessorFactory(false, funcRegistry)
            };

            TVector<NActors::TActorId> ytRM;
            ytRM.reserve(communalBackendCount);

            TVector<TResourceManagerOptions> uploadResourcesOptions;

            auto nodesPerCluster = static_cast<ui32>(NDqs::ENodeIdLimits::MaxWorkerNodeId) - static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId);
            nodesPerCluster /= Max<ui32>(communalBackendCount, 1);
            auto startNodeId = static_cast<ui32>(NDqs::ENodeIdLimits::MinWorkerNodeId);

            TString solomonToken;
            if (dqConfig.HasSolomon() && dqConfig.GetSolomon().HasTokenFile()) {
                solomonToken = TFileInput(dqConfig.GetSolomon().GetTokenFile()).ReadLine();
            }

            for (const auto& ytConfig : dqConfig.GetYtBackends()) {
                if (!IsCommunalYtBackend(ytConfig)) {
                    continue;
                }

                TResourceManagerOptions rmOptions;
                rmOptions.YtBackend = NormalizeYtBackendCredentials(ytConfig);
                if (!rmOptions.YtBackend.HasICSettings()) {
                    *rmOptions.YtBackend.MutableICSettings() = dqConfig.GetICSettings();
                }
                if (!rmOptions.YtBackend.HasPortoSettings()) {
                    *rmOptions.YtBackend.MutablePortoSettings() = dqConfig.GetPortoSettings();
                }
                if (!rmOptions.YtBackend.HasSolomon() && dqConfig.HasSolomon()) {
                    *rmOptions.YtBackend.MutableSolomon() = dqConfig.GetSolomon();
                    if (solomonToken) {
                        rmOptions.YtBackend.MutableSolomon()->SetToken(solomonToken);
                    }
                }
                if (!rmOptions.YtBackend.HasProxyAddress()) {
                    *rmOptions.YtBackend.MutableProxyAddress() = rmOptions.YtBackend.GetClusterName();
                }

                if (!rmOptions.YtBackend.HasPrefix()) {
                    Y_ABORT_UNLESS(rmOptions.YtBackend.HasUploadPrefix());

                    auto prefix = rmOptions.YtBackend.GetUploadPrefix();
                    auto pos = prefix.rfind("/");
                    if (pos != TString::npos) {
                        prefix = prefix.substr(0, pos);
                    }
                    rmOptions.YtBackend.SetPrefix(prefix);
                }

                if (!rmOptions.YtBackend.HasUploadPrefix()) {
                    rmOptions.YtBackend.SetUploadPrefix(rmOptions.YtBackend.GetPrefix() + "/tmp");
                }

                if (!rmOptions.YtBackend.HasMinNodeId() || !rmOptions.YtBackend.HasMaxNodeId()) {
                    rmOptions.YtBackend.SetMinNodeId(startNodeId);
                    rmOptions.YtBackend.SetMaxNodeId(startNodeId + nodesPerCluster);
                    startNodeId += nodesPerCluster;
                }

                const auto& vanillaJobFiles = rmOptions.YtBackend.GetVanillaJobFile().empty()
                    ? dqConfig.GetYtBackends(0).GetVanillaJobFile()
                    : rmOptions.YtBackend.GetVanillaJobFile();
                const auto& vanillaJobLite = rmOptions.YtBackend.GetVanillaJobLite().empty()
                    ? dqConfig.GetYtBackends(0).GetVanillaJobLite()
                    : rmOptions.YtBackend.GetVanillaJobLite();

                // Add vanilla job starter and other required files for upload.
                for (const auto& jobFile : vanillaJobFiles) {
                    rmOptions.Files.push_back(jobFile.GetLocalPath());
                }
                // COMPAT
                if (!localhostMode /*&& (!dqConfig.HasControl() || dqConfig.GetControl().GetDisabled())*/) {
                    // for backward compatibility. Duplication in IsReady method
                    rmOptions.Files.push_back(vanillaJobLite);
                }

                // uploader
                rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/bin/" + ToString(GetProgramCommitId());
                if (rmOptions.YtBackend.GetClusterName().find("localhost") != 0) {
                    // don't start uploader for local-yt
                    rmOptions.LockName = TString("ytuploader.") + rmOptions.YtBackend.GetClusterName();
                    rmOptions.Counters = metricsRegistry->GetSensors()->GetSubgroup("counters", "uploader");
                    ytRM.push_back(actorSystem->Register(CreateResourceUploader(rmOptions, coordinator)));
                }
                {
                    // bin cleaner
                    rmOptions.KeepFirst = 10;
                    rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/bin";
                    ytRM.push_back(actorSystem->Register(CreateResourceCleaner(rmOptions, coordinator)));
                }
                {
                    // udf cleaner
                    rmOptions.KeepFirst = 500;
                    rmOptions.KeepFilter = ".tmp"; // don't remove inprogress files
                    rmOptions.DropBefore = TDuration::Days(7);
                    rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/udfs";
                    ytRM.push_back(actorSystem->Register(CreateResourceCleaner(rmOptions, coordinator)));
                }
                {
                    // temporary locks
                    rmOptions.KeepFilter = rmOptions.YtBackend.GetClusterName(); // don't remove locks with `ClusterName` in LockName
                    rmOptions.DropBefore = TDuration::Hours(1);
                    rmOptions.UploadPrefix = rmOptions.YtBackend.GetPrefix() + "/locks";
                    ytRM.push_back(actorSystem->Register(CreateResourceCleaner(rmOptions, coordinator)));
                }

                // rm manager
                rmOptions.Files.clear();
                // Add vanilla job starter and other files required for execution
                for (const auto& jobFile : vanillaJobFiles) {
                    rmOptions.Files.push_back(jobFile.GetLocalPath());
                }

                rmOptions.LockName = TString("ytrm.") + rmOptions.YtBackend.GetClusterName();
                rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix() + "/bin/" + ToString(GetProgramCommitId());
                rmOptions.Counters = metricsRegistry->GetSensors()->GetSubgroup("counters", "ytrm")->GetSubgroup("ytname", rmOptions.YtBackend.GetClusterName());
                ytRM.push_back(actorSystem->Register(CreateResourceManager(rmOptions, coordinator)));

                rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix();
                rmOptions.Files.clear();
                uploadResourcesOptions.push_back(rmOptions);
            }
            // Use the legacy GWM actor id until all clients switch to MakeGlobalWorkerManagerActorID.
            const auto globalWorkerManagerActorId = NDqs::MakeWorkerManagerActorID(nodeId);
            YQL_ENSURE(globalWorkerManagerStarted == !uploadResourcesOptions.empty());
            if (globalWorkerManagerStarted) {
                coordinator->StartGlobalWorker(actorSystem, uploadResourcesOptions, metricsRegistry);
                YQL_CLOG(INFO, ProviderDq) << "DQ gRPC will forward to GWM actor "
                    << globalWorkerManagerActorId
                    << " serviceNodeId=" << nodeId;
                serviceNode->StartService(dqTaskPreprocessorFactories);
            } else {
                YQL_CLOG(INFO, ProviderDq) << "No communal YT backends (MaxJobs=0), skipping GWM";
            }

            YQL_CLOG(INFO, ProviderDq) << "dq process start waiting for signal";
            const auto IdleTime = TDuration::Seconds(2);
            while (true) {
                if (NeedTerminate || NeedQuit) {
                    AddProcTitleSuffix("[terminating]");
                    break;
                }
                ReopenLogIfNeeded(Config);
                Sleep(IdleTime);
            }

            YQL_CLOG(INFO, ProviderDq) << "dq process will stop ytrm and gwm";
            for (auto ytrmId : ytRM) {
                actorSystem->Send(ytrmId, new NActors::TEvents::TEvPoison);
            }
            if (globalWorkerManagerStarted) {
                // Cookie 1 switches GWM to terminating mode.
                actorSystem->Send(
                    new IEventHandle(
                        globalWorkerManagerActorId,
                        TActorId(),
                        new TEvBecomeFollower,
                        /*flag=*/0,
                        /*cookie=*/1));
            }

            YQL_CLOG(INFO, ProviderDq) << "waiting stop signal";
            while (true) {
                if (NeedQuit) {
                    AddProcTitleSuffix("[quitting]");
                    break;
                }
                ReopenLogIfNeeded(Config);
                Sleep(IdleTime);
            }

            YQL_CLOG(INFO, ProviderDq) << "dq process will shutdown";

            if (globalWorkerManagerStarted) {
                actorSystem->Send(globalWorkerManagerActorId, new NActors::TEvents::TEvPoison);
            }
            if (metricsPusherId) {
                actorSystem->Send(metricsPusherId, new NActors::TEvents::TEvPoison);
            }
            coordinator->Stop(actorSystem); // async

            serviceNode->Stop(TDuration::Seconds(5));
        } catch (...) {
            YQL_CLOG(ERROR, ProviderDq) << "unhandled exception: "
                           << CurrentExceptionMessage();
            exitCode = 1;
        }

        YQL_CLOG(INFO, ProviderDq) << "dq process shutdown";

        YtShutdown();

        return exitCode;
    }
};

TWorkerDq::TWorkerDq(
    const TMainConfig& config,
    const TBindResultVector& serverPorts)
    : Impl_(new TImpl(config, serverPorts))
{
}

TWorkerDq::~TWorkerDq()
{
}

int TWorkerDq::Run()
{
    return Impl_->Run();
}

} // namespace NYql
