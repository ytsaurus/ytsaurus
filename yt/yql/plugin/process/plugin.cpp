#include "private.h"

#include "config.h"

#include "plugin.h"
#include "process.h"

#include <yt/yql/plugin/config.h>
#include <yt/yql/plugin/bridge/plugin.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/yql_plugin/yql_plugin_proxy.h>

#include <yt/yt/library/process/process.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>
#include <library/cpp/yt/logging/backends/arcadia/backend.h>

namespace NYT::NYqlPlugin::NProcess {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NThreading;
using namespace NYson;
using namespace NYTree;

using NYqlClient::NProto::TYqlQueryFile_EContentType;
using NYqlClient::NProto::TYqlResponse;


static constexpr auto& Logger = ProcessYqlPluginLogger;
constexpr int MODE0711 = S_IRWXU | S_IXGRP | S_IXOTH;

////////////////////////////////////////////////////////////////////////////////

/**
 * Implementation of IYqlPlugin that managees a pool of subprocesses for query
 * execution. Each subprocess handles exactly one query and it is killed after
 * query finishes its execution.
*/
class TProcessYqlPlugin
    : public IYqlPlugin
{
public:
    TProcessYqlPlugin(
        TYqlPluginConfigPtr config,
        TSingletonsConfigPtr singletonsConfig,
        TConnectionCompoundConfigPtr clusterConnectionConfig,
        TString maxSupportedYqlVersion,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , ConfigTemplate_(BuildPluginConfigTemplate(Config_, singletonsConfig, clusterConnectionConfig, std::move(maxSupportedYqlVersion)))
        , DynamicConfigVersion_(0)
        , Queue_(New<TActionQueue>("YqlProcessPlugin"))
        , Invoker_(Queue_->GetInvoker())
        , StartProcessesThreadPool_(CreateThreadPool(64, "YqlProcessPluginStart"))
        , StartProcessInvoker_(StartProcessesThreadPool_->GetInvoker())
        , StandbyProcessesQueue_(Config_->ProcessPluginConfig->SlotCount)
        , StandbyProcessesGauge_(profiler.Gauge("/standby_processes"))
        , ActiveProcessesGauge_(profiler.Gauge("/active_processes"))
        , ProcessesLimitGauge_(profiler.Gauge("/processes_limit"))
    {
        if (Config_->EnableDQ) {
            InitializeDqControllerYqlPlugin(singletonsConfig, maxSupportedYqlVersion);
        }
        ProcessesLimitGauge_.Update(Config_->ProcessPluginConfig->SlotCount);
        InitializeProcessPool();
    }

    void Start() override
    {
        if (DqControllerYqlPlugin_) {
            DqControllerYqlPlugin_->Start();
        }
    }

    // Acquires subprocess for query and routes call to it. Acquired process will
    // also be used in subsequent Run call. If Run call did not happen in one
    // minute, the process will be reinitialized.
    TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files) override
    {
        // Yql agent calls this method first when staring query, so we acquire
        // process in this call and then use it in Run() as well.
        TYqlExecutorProcessPtr acquiredProcess = AcquireSlotForQuery(queryId);

        if (!acquiredProcess) {
            return TClustersResult{
                .YsonError = ConvertToYsonString(TError("No available slots to acquire")).ToString()
            };
        }

        YT_LOG_INFO("Acquired slot for query (SlotIndex: %v, QueryId: %v)", acquiredProcess->SlotIndex(), queryId);

        return acquiredProcess->GetUsedClusters(queryId, queryText, settings, files);
    }

    // Gets an acquired in GetUsedClusters subprocess and routes Run call to it.
    // Marks subprocess as active so it would not reinitialize. Long blocking call
    TQueryResult Run(
        TQueryId queryId,
        TString user,
        TYsonString credentials,
        TString queryText,
        TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);

        if (!pluginProcessOrError.IsOK()) {
            return TQueryResult{
                .YsonError = ConvertToYsonString<TError>(pluginProcessOrError).ToString()
            };
        }

        auto pluginProcess = pluginProcessOrError.Value();

        YT_LOG_INFO("Starting query in subprocess (QueryId: %v, SlotIndex: %v)", queryId, pluginProcess->SlotIndex());

        auto finishQueryGuard = Finally(BIND(&TProcessYqlPlugin::OnQueryFinish, this, queryId, pluginProcess)
            .Via(Invoker_));

        auto result = pluginProcess->Run(
            queryId,
            user,
            credentials,
            queryText,
            settings,
            files,
            executeMode);

        YT_LOG_INFO("Query finished (QueryId: %v, SlotIndex: %v)", queryId, pluginProcess->SlotIndex());
        return result;
    }

    TQueryResult GetProgress(TQueryId queryId) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);
        if (!pluginProcessOrError.IsOK()) {
            return TQueryResult{
                .YsonError = ConvertToYsonString<TError>(pluginProcessOrError).ToString()
                };
        }

        auto pluginProcess = pluginProcessOrError.Value();
        return pluginProcess->GetProgress(queryId);
    }

    TAbortResult Abort(TQueryId queryId) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);
        if (!pluginProcessOrError.IsOK()) {
            return TAbortResult{
                .YsonError = ConvertToYsonString<TError>(pluginProcessOrError).ToString()
            };
        }

        auto pluginProcess = pluginProcessOrError.Value();
        return pluginProcess->Abort(queryId);
    }

    TGetDeclaredParametersInfoResult GetDeclaredParametersInfo(
        TQueryId queryId,
        TString user,
        TString queryText,
        TYsonString settings,
        TYsonString credentials) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);
        if (!pluginProcessOrError.IsOK()) {
            THROW_ERROR pluginProcessOrError;
        }

        auto pluginProcess = pluginProcessOrError.Value();

        auto finishQueryGuard = Finally(BIND(&TProcessYqlPlugin::OnQueryFinish, this, queryId, pluginProcess)
            .Via(Invoker_));

        return pluginProcess->GetDeclaredParametersInfo(
            queryId,
            user,
            queryText,
            settings,
            credentials);
    }

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) override
    {
        auto guard = WriterGuard(ProcessesLock_);
        YT_LOG_INFO("Updating dynamic config");

        CurrentDynamicGatewaysConfig_ = config.GatewaysConfig;
        ++DynamicConfigVersion_;

        if (config.MaxSupportedYqlVersion) {
            CurrentDynamicMaxYqlLangVersion_ = config.MaxSupportedYqlVersion.ToString();
        } else {
            CurrentDynamicMaxYqlLangVersion_.reset();
        }

        if (DqControllerYqlPlugin_) {
            DqControllerYqlPlugin_->OnDynamicConfigChanged(config);
        }

        while (!StandbyProcessesQueue_.Empty()) {
            auto process = *StandbyProcessesQueue_.Pop();
            YT_LOG_DEBUG("Stopping process for dynamic config update (SlotIndex: %v)", process->SlotIndex());
            process->Stop();
        }

        YT_LOG_INFO("Dynamic config updated");
    }

    IMapNodePtr GetOrchidNode() const override
    {
        auto guard = ReaderGuard(ProcessesLock_);
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("active_processes").Value(RunningYqlQueries_.size())
                .Item("standby_processes").Value(StandbyProcessesQueue_.Size())
                .Item("total_processes").Value(Config_->ProcessPluginConfig->SlotCount)
            .EndMap()->AsMap();
    }

private:
    static TString SocketName;

    TYqlPluginConfigPtr Config_;
    TProcessYqlPluginInternalConfigPtr ConfigTemplate_;

    int DynamicConfigVersion_;
    std::optional<TYsonString> CurrentDynamicGatewaysConfig_;
    std::optional<TString> CurrentDynamicMaxYqlLangVersion_;

    TActionQueuePtr Queue_;
    IInvokerPtr Invoker_;

    IThreadPoolPtr StartProcessesThreadPool_;
    IInvokerPtr StartProcessInvoker_;

    std::unique_ptr<IYqlPlugin> DqControllerYqlPlugin_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, ProcessesLock_);
    THashMap<TQueryId, TYqlExecutorProcessPtr> RunningYqlQueries_;
    ::NThreading::TBlockingQueue<TYqlExecutorProcessPtr> StandbyProcessesQueue_;

    TGauge StandbyProcessesGauge_;
    TGauge ActiveProcessesGauge_;
    TGauge ProcessesLimitGauge_;


    TYqlExecutorProcessPtr AcquireSlotForQuery(TQueryId queryId) noexcept
    {
        auto acquiredProcess = StandbyProcessesQueue_.Pop();

        if (!acquiredProcess) {
            YT_LOG_ERROR("Standby processes queue has been shutdown; Can't acquire process for query (QueryId: %v)", queryId);
            return nullptr;
        }

        auto guard = WriterGuard(ProcessesLock_);

        RunningYqlQueries_[queryId] = *acquiredProcess;

        StandbyProcessesGauge_.Update(StandbyProcessesQueue_.Size());
        ActiveProcessesGauge_.Update(RunningYqlQueries_.size());

        // We should check whether query was started in process. If it did not, then
        // we need to restart process, because otherwise it will be stuck forever.
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TProcessYqlPlugin::CheckProcessIsActive, this, *acquiredProcess, queryId),
            Config_->ProcessPluginConfig->CheckProcessActiveDelay,
            Invoker_);

        return *acquiredProcess;
    }

    TErrorOr<TYqlExecutorProcessPtr> GetYqlPluginByQueryId(TQueryId queryId)
    {
        auto guard = ReaderGuard(ProcessesLock_);

        if (!RunningYqlQueries_.contains(queryId)) {
            YT_LOG_WARNING("Query was not found in running queries (QueryId: %v)", queryId);
            return TError("Query %v was not found in running queries", queryId);
        }

        return RunningYqlQueries_[queryId];
    }

    void CleanupAfterQueryFinish(std::optional<TQueryId> queryId)
    {
        auto guard = WriterGuard(ProcessesLock_);
        if (queryId) {
            RunningYqlQueries_.erase(*queryId);
        }
        ActiveProcessesGauge_.Update(RunningYqlQueries_.size());
        StandbyProcessesGauge_.Update(StandbyProcessesQueue_.Size());
    }

    void CheckProcessIsActive(TYqlExecutorProcessPtr process, TGuid queryId)
    {
        if (process->ActiveQueryId()) {
            return;
        }
        YT_LOG_WARNING("Query did not start after acquiring process; restarting process (SlotIndex: %v, QueryId: %v)", process->SlotIndex(), queryId);
        process->Stop();
        CleanupAfterQueryFinish(queryId);
    }

    void StartPluginInProcess(TYqlExecutorProcessPtr process)
    {
        int slotIndex = process->SlotIndex();
        YT_LOG_DEBUG("Starting plugin in process (SlotIndex: %v)", slotIndex);

        bool success = process->WaitReady();

        auto restartProcess = [this, process, slotIndex]() {
            process->Stop();
            YT_UNUSED_FUTURE(StartPluginProcess(slotIndex));
        };

        if (!success) {
            YT_LOG_ERROR("All attempts to start plugin in process failed, restarting process (SlotIndex: %v)", slotIndex);
            restartProcess();
            return;
        }

        auto guard = WriterGuard(ProcessesLock_);
        if (process->DynamicConfigVersion() < DynamicConfigVersion_) {
            YT_LOG_DEBUG("Dynamic config was updated during process start; restarting process (SlotIndex: %v)", slotIndex);
            restartProcess();
            return;
        }

        StandbyProcessesQueue_.Push(process);
        StandbyProcessesGauge_.Update(StandbyProcessesQueue_.Size());

        process->SubscribeOnFinish(
            BIND([slotIndex, process, this](const TErrorOr<void> result) {
                YT_LOG_DEBUG(result, "Process finished (SlotIndex: %v)", slotIndex);
                CleanupAfterQueryFinish(process->ActiveQueryId());
                YT_UNUSED_FUTURE(StartPluginProcess(slotIndex));
            }).Via(Invoker_));

        YT_LOG_INFO("Successfully started plugin in subprocess (SlotIndex: %v)", slotIndex);
    }

    void OnQueryFinish(TQueryId queryId, TYqlExecutorProcessPtr process)
    {
        YT_LOG_DEBUG("Query finished, cleaning up and restarting process (QueryId: %v, SlotIndex: %v)", queryId, process->SlotIndex());
        CleanupAfterQueryFinish(queryId);
        process->Stop();
    }

    void InitializeProcessPool()
    {
        try {
            YT_LOG_INFO("Initializing process pool");
            std::vector<TFuture<void>> futures;
            for (int i = 0; i < Config_->ProcessPluginConfig->SlotCount; ++i) {
                futures.emplace_back(StartPluginProcess(i));
            }

            WaitFor(AllSucceeded(futures)).ThrowOnError();
            YT_LOG_INFO("Process pool initialized");
        } catch (std::exception e) {
            YT_LOG_ERROR(e, "Failed to initialize");
        }
    }

    TFuture<void> StartPluginProcess(int slotIndex)
    {
        YT_LOG_INFO("Starting yql plugin process (SlotIndex: %v)", slotIndex);

        return BIND_NO_PROPAGATE(&TProcessYqlPlugin::StartProcess, Passed(this), slotIndex)
            .AsyncVia(StartProcessInvoker_)
            .Run()
            .Apply(BIND_NO_PROPAGATE([this, slotIndex](const TErrorOr<TYqlExecutorProcessPtr>& result) {
                if (!result.IsOK()) {
                    YT_LOG_WARNING(result, "Failed to start yql plugin process, trying again (SlotIndex: %v)", slotIndex);
                    YT_UNUSED_FUTURE(StartPluginProcess(slotIndex));
                    return;
                }
                YT_LOG_DEBUG("Successfully started process, starting yql plugin in process (SlotIndex: %v)", slotIndex);
                StartPluginInProcess(result.Value());
            }));
    }

    TString GetSlotPath(int slotIndex) const
    {
        return NFS::CombinePaths({Config_->ProcessPluginConfig->SlotsRootPath, Format("%v", slotIndex)});
    }

    TString GetUnixDomainSocketPath(int slotIndex) const
    {
        return NFS::CombinePaths(GetSlotPath(slotIndex), SocketName);
    }

    TYqlExecutorProcessPtr StartProcess(int slotIndex)
    {
        YT_LOG_DEBUG("Starting plugin process (SlotIndex: %v)", slotIndex);
        auto guard = ReaderGuard(ProcessesLock_);
        auto workingDirectory = GetSlotPath(slotIndex);
        NFS::MakeDirRecursive(workingDirectory, MODE0711);
        auto unixDomainSocketPath = GetUnixDomainSocketPath(slotIndex);

        if (NFS::Exists(unixDomainSocketPath)) {
            NFS::Remove(unixDomainSocketPath);
        }

        auto serverConfig = NBus::TBusServerConfig::CreateUds(unixDomainSocketPath);
        auto clientConfig = NBus::TBusClientConfig::CreateUds(unixDomainSocketPath);

        auto config = BuildProcessConfig(slotIndex, serverConfig);

        auto client = NBus::CreateBusClient(clientConfig);

        auto process = New<TSimpleProcess>(YqlAgentProgramName);

        WriteConfig(config, NFS::CombinePaths(workingDirectory, YqlPluginConfigFilename));

        process->SetWorkingDirectory(workingDirectory);
        process->AddArguments({
            YqlPluginSubcommandName,
            "--config", YqlPluginConfigFilename
        });

        auto proxy = TYqlPluginProxy(NRpc::NBus::CreateBusChannel(client));
        proxy.SetDefaultTimeout(Config_->ProcessPluginConfig->DefaultRequestTimeout);

        return New<TYqlExecutorProcess>(
            slotIndex,
            DynamicConfigVersion_,
            std::move(proxy),
            unixDomainSocketPath,
            process,
            process->Spawn(),
            Config_->ProcessPluginConfig->RunRequestTimeout);
    }

    TProcessYqlPluginInternalConfigPtr BuildProcessConfig(int slotIndex, NBus::TBusServerConfigPtr serverConfig)
    {
        auto config = NYTree::CloneYsonStruct(ConfigTemplate_);

        config->SlotIndex = slotIndex;
        config->BusServer = serverConfig;

        auto logManagerConfig = config->SingletonsConfig->GetSingletonConfig<NLogging::TLogManagerConfig>();
        logManagerConfig->UpdateWriters([&](const NYTree::IMapNodePtr& writerConfigNode) {
            auto writerConfig = ConvertTo<NLogging::TLogWriterConfigPtr>(writerConfigNode);
            if (writerConfig->Type != NLogging::TFileLogWriterConfig::WriterType) {
                return writerConfigNode;
            }

            auto fileLogWriterConfig = ConvertTo<NLogging::TFileLogWriterConfigPtr>(writerConfigNode);
            TString logsDirectory = NFS::CombinePaths(GetSlotPath(slotIndex), "logs");
            NFS::MakeDirRecursive(logsDirectory, MODE0711);
            fileLogWriterConfig->FileName = NFS::CombinePaths(logsDirectory, NFS::GetFileName(fileLogWriterConfig->FileName));

            return ConvertTo<NYTree::IMapNodePtr>(fileLogWriterConfig);
        });

        config->SetSingletonConfig(config->SingletonsConfig->GetSingletonConfig<NLogging::TLogManagerConfig>());

        config->DynamicGatewaysConfig = CurrentDynamicGatewaysConfig_;

        if (CurrentDynamicMaxYqlLangVersion_.has_value()) {
            config->MaxSupportedYqlVersion = CurrentDynamicMaxYqlLangVersion_.value();
        }

        return config;
    }

    void WriteConfig(TProcessYqlPluginInternalConfigPtr config, TString configPath)
    {
        try {
            TFile file(configPath, CreateAlways | WrOnly | Seq | CloseOnExec);
            TUnbufferedFileOutput output(file);
            TYsonWriter writer(&output, EYsonFormat::Pretty);
            Serialize(config, &writer);
            writer.Flush();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to write process config");
            throw;
        }

        YT_LOG_DEBUG("Yql plugin config successfully written (SlotIndex: %v, ConfigPath: %v)", config->SlotIndex, configPath);
    }

    static TProcessYqlPluginInternalConfigPtr BuildPluginConfigTemplate(
        TYqlPluginConfigPtr config,
        TSingletonsConfigPtr singletonsConfig,
        TConnectionCompoundConfigPtr clusterConnectionConfig,
        TString maxSupportedYqlVersion)
    {
        auto result = New<TProcessYqlPluginInternalConfig>();

        auto pluginSingletonsConfig = CloneYsonStruct(singletonsConfig);

        pluginSingletonsConfig->SetSingletonConfig(config->ProcessPluginConfig->LogManagerTemplate);

        result->SingletonsConfig = pluginSingletonsConfig;
        result->ClusterConnection = clusterConnectionConfig;

        result->PluginConfig = config;
        result->MaxSupportedYqlVersion = maxSupportedYqlVersion;

        return result;
    }

    void InitializeDqControllerYqlPlugin(TSingletonsConfigPtr singletonsConfig, std::string maxSupportedYqlVersion)
    {
        auto options = ConvertToOptions(
            Config_,
            ConvertToYsonString(singletonsConfig),
            NYT::NLogging::CreateArcadiaLogBackend(NLogging::TLogger("YqlPlugin")),
            maxSupportedYqlVersion,
            true);
        DqControllerYqlPlugin_ = CreateBridgeYqlPlugin(std::move(options));
    }
};

////////////////////////////////////////////////////////////////////////////////

TString TProcessYqlPlugin::SocketName = "yql-plugin.sock";

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateProcessYqlPlugin(
    TYqlPluginConfigPtr pluginConfig,
    TSingletonsConfigPtr singletonsConfig,
    TConnectionCompoundConfigPtr clusterConnectionConfig,
    TString maxSupportedYqlVersion,
    const NProfiling::TProfiler& profiler)
{
    return std::make_unique<TProcessYqlPlugin>(std::move(pluginConfig), singletonsConfig, clusterConnectionConfig, maxSupportedYqlVersion, profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NProcess
