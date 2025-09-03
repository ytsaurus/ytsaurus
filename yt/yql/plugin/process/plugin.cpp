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
        NYson::TYsonString settings,
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
        NYson::TYsonString credentials,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);

        if (!pluginProcessOrError.IsOK()) {
            return TQueryResult{
                .YsonError = ConvertToYsonString<TError>(pluginProcessOrError).ToString()
            };
        }

        auto finishQueryGuard = Finally(BIND(&TProcessYqlPlugin::OnQueryFinish, this, queryId, pluginProcessOrError.Value())
            .Via(Invoker_));

        return pluginProcessOrError.Value()->Run(
            queryId,
            user,
            credentials,
            queryText,
            settings,
            files,
            executeMode);
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

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) override
    {
        YT_LOG_INFO("Updating dynamic config");
        auto guard = NThreading::WriterGuard(ProcessesLock_);
        UpdateConfigTemplateOnConfigChanged(config);
        ++DynamicConfigVersion_;

        if (DqControllerYqlPlugin_) {
            DqControllerYqlPlugin_->OnDynamicConfigChanged(config);
        }

        while (StandbyProcessesQueue_.Empty()) {
            auto process = *StandbyProcessesQueue_.Pop();
            process->Stop();
            CleanupAfterQueryFinish(process->ActiveQueryId());
        }

        YT_LOG_INFO("Dynamic config updated");
    }

    NYTree::IMapNodePtr GetOrchidNode() const override
    {
        auto guard = NThreading::ReaderGuard(ProcessesLock_);
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

    TActionQueuePtr Queue_;
    IInvokerPtr Invoker_;

    std::unique_ptr<IYqlPlugin> DqControllerYqlPlugin_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProcessesLock_);
    THashMap<TQueryId, TYqlExecutorProcessPtr> RunningYqlQueries_;
    ::NThreading::TBlockingQueue<TYqlExecutorProcessPtr> StandbyProcessesQueue_;

    NProfiling::TGauge StandbyProcessesGauge_;
    NProfiling::TGauge ActiveProcessesGauge_;
    NProfiling::TGauge ProcessesLimitGauge_;

private:
    TYqlExecutorProcessPtr AcquireSlotForQuery(TQueryId queryId) noexcept
    {
        auto acquiredProcess = StandbyProcessesQueue_.Pop();

        if (!acquiredProcess) {
            YT_LOG_ERROR("Standby processes queue has been shutdown; Can't acquire process for query (QueryId: %v)", queryId);
            return nullptr;
        }

        auto guard = NThreading::WriterGuard(ProcessesLock_);

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
        auto guard = NThreading::ReaderGuard(ProcessesLock_);

        if (!RunningYqlQueries_.contains(queryId)) {
            YT_LOG_WARNING("Query was not found in running queries (QueryId: %v)", queryId);
            return TError("Query %v was not found in running queries", queryId);
        }

        return RunningYqlQueries_[queryId];
    }

    void CleanupAfterQueryFinish(std::optional<TQueryId> queryId)
    {
        auto guard = NThreading::WriterGuard(ProcessesLock_);
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
        YT_LOG_WARNING("Query did not start after acquiring process; Restarting process (SlotIndex: %v, QueryId: %v)", process->SlotIndex(), queryId);
        process->Stop();
        CleanupAfterQueryFinish(queryId);
    }

    void StartPluginInProcess(TYqlExecutorProcessPtr process)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        int slotIndex = process->SlotIndex();
        YT_LOG_INFO("Starting plugin in process (SlotIndex: %v)", slotIndex);

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

        auto guard = NThreading::WriterGuard(ProcessesLock_);
        if (process->DynamicConfigVersion() < DynamicConfigVersion_) {
            YT_LOG_DEBUG("Dynamic config was updated during process start; Restarting process (SlotIndex: %v)", slotIndex);
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
        YT_LOG_DEBUG("Query finished, clearing slot and restarting process (QueryId: %v, SlotIndex: %v)", queryId, process->SlotIndex());
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

        return BIND_NO_PROPAGATE(&TProcessYqlPlugin::StartProcess, this, slotIndex)
            .AsyncVia(Invoker_)
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
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_LOG_INFO("Starting plugin process (SlotIndex: %v)", slotIndex);
        auto guard = NThreading::ReaderGuard(ProcessesLock_);
        TString workingDirectory = GetSlotPath(slotIndex);
        NFS::MakeDirRecursive(workingDirectory, MODE0711);
        TString unixDomainSocketPath = GetUnixDomainSocketPath(slotIndex);

        if (NFS::Exists(unixDomainSocketPath)) {
            NFS::Remove(unixDomainSocketPath);
        }

        auto serverConfig = NBus::TBusServerConfig::CreateUds(unixDomainSocketPath);
        auto clientConfig = NBus::TBusClientConfig::CreateUds(unixDomainSocketPath);

        TProcessYqlPluginInternalConfigPtr config = BuildProcessConfig(slotIndex, serverConfig);

        auto client = NBus::CreateBusClient(clientConfig);

        TProcessBasePtr process = New<TSimpleProcess>(YqlAgentProgramName);

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
        TProcessYqlPluginInternalConfigPtr config = NYTree::CloneYsonStruct(ConfigTemplate_);

        config->SlotIndex = slotIndex;
        config->BusServer = serverConfig;

        auto fileStoragePath = BuildYsonNodeFluently()
            .Value(NFS::CombinePaths(GetSlotPath(slotIndex), "tmp"));

        config->PluginConfig->FileStorageConfig->AsMap()->AddChild("path", fileStoragePath);

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

    void UpdateConfigTemplateOnConfigChanged(const TYqlPluginDynamicConfig& config)
    {
        NYson::TProtobufWriterOptions protobufWriterOptions;
        protobufWriterOptions.ConvertSnakeToCamelCase = true;

        ConfigTemplate_->PluginConfig->GatewayConfig = PatchNode(
            ConfigTemplate_->PluginConfig->GatewayConfig,
            ConvertTo<INodePtr>(config.GatewaysConfig));
    }

    static TProcessYqlPluginInternalConfigPtr BuildPluginConfigTemplate(
        TYqlPluginConfigPtr config,
        TSingletonsConfigPtr singletonsConfig,
        TConnectionCompoundConfigPtr clusterConnectionConfig,
        TString maxSupportedYqlVersion)
    {
        TProcessYqlPluginInternalConfigPtr result = New<TProcessYqlPluginInternalConfig>();

        TSingletonsConfigPtr pluginSingletonsConfig = CloneYsonStruct(singletonsConfig);

        pluginSingletonsConfig->SetSingletonConfig(config->ProcessPluginConfig->LogManagerTemplate);

        result->SingletonsConfig = pluginSingletonsConfig;
        result->ClusterConnection = clusterConnectionConfig;

        result->PluginConfig = config;
        result->MaxSupportedYqlVersion = maxSupportedYqlVersion;

        return result;
    }

    void InitializeDqControllerYqlPlugin(TSingletonsConfigPtr singletonsConfig, std::string maxSupportedYqlVersion)
    {
        TYqlPluginOptions options = ConvertToOptions(
            Config_,
            ConvertToYsonString(singletonsConfig),
            NYT::NLogging::CreateArcadiaLogBackend(NLogging::TLogger("YqlPlugin")),
            maxSupportedYqlVersion,
            true);
        DqControllerYqlPlugin_ = CreateBridgeYqlPlugin(std::move(options));
    }
};

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

} // namespace NYT::NYqlPlugin::NProcess
