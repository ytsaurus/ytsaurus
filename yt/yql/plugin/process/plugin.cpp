#include "plugin.h"

#include "private.h"

#include <library/cpp/retry/retry.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/yql_plugin/yql_plugin_proxy.h>

#include <yt/yt/library/process/process.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

static constexpr auto& Logger = YqlProcessPluginLogger;

using namespace NConcurrency;
using namespace NYson;

using NYqlClient::NProto::TYqlQueryFile;
using NYqlClient::NProto::TYqlQueryFile_EContentType;
using NYqlClient::NProto::TYqlResponse;

namespace {

DECLARE_REFCOUNTED_STRUCT(TYqlPluginRunningProcess);
struct TYqlPluginRunningProcess
    : public TRefCounted
{
    TYqlPluginRunningProcess(
        int slotIndex,
        TYqlPluginProcessInternalConfigPtr configSnapshot,
        TProcessBasePtr process,
        TFuture<void> result,
        TYqlPluginProxy proxy)
        : SlotIndex(slotIndex)
        , ConfigSnapshot(std::move(configSnapshot))
        , Process(std::move(process))
        , Result(std::move(result))
        , PluginProxy(std::move(proxy))
        , IsActive(false)
    { }

    int SlotIndex;
    TYqlPluginProcessInternalConfigPtr ConfigSnapshot;
    TProcessBasePtr Process;
    TFuture<void> Result;
    TYqlPluginProxy PluginProxy;
    std::atomic<bool> IsActive;
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginRunningProcess)
constexpr int MODE0711 = S_IRWXU | S_IXGRP | S_IXOTH;

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Implementation of IYqlPlugin that managees a pool of subprocesses for query
// execution. Each subprocess handles exactly one query and it is killed after
// query finishes its execution.
class TYqlProcessPlugin
    : public IYqlPlugin
{
public:
    explicit TYqlProcessPlugin(
        NYqlAgent::TBootstrap* bootstrap,
        TYqlPluginOptions options,
        TYqlProcessPluginConfigPtr config,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , Queue_(New<TActionQueue>("YqlProcessPlugin"))
        , Invoker_(Queue_->GetInvoker())
        , YqlProcesses_(Config_->SlotsCount)
        , ConfigTemplate_(BuildPluginConfigTemplate(Config_, std::move(options), bootstrap))
        , StandbyProcessesGauge_(profiler.Gauge("/standby_processes"))
        , ActiveProcessesGauge_(profiler.Gauge("/active_processes"))
        , ProcessesLimitGauge_(profiler.Gauge("/processes_limit"))
    {
        ProcessesLimitGauge_.Update(Config_->SlotsCount);
        InitializeProcessPool();
    }

    void Start() override
    {
        std::vector<TFuture<void>> futures;
        auto guard = NThreading::ReaderGuard(ProcessesLock_);
        for (int slotIndex = 0; slotIndex < Config_->SlotsCount; ++slotIndex) {
            futures.push_back(
                BIND(&TYqlProcessPlugin::StartPluginInProcess, this, YqlProcesses_[slotIndex])
                    .AsyncVia(Invoker_)
                    .Run());
        }
        guard.Release();

        WaitFor(AllSucceeded(futures)).ThrowOnError();
    }

    // Acquires subprocess for query and routes call to it. Acquired process will also be used
    // in subsequent Run call. If Run call did not happen in one minute, the process will be reinitialized.
    TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files) override
    {
        // Yql agent calls this method first when staring query, so we acquire process
        // in this call and then use it in Run() as well.
        TYqlPluginRunningProcessPtr acquiredProcess = AcquireSlotForQuery(queryId);

        if (!acquiredProcess) {
            return TClustersResult{
                .YsonError = ConvertToYsonString(TError("No available slots to acquire")).ToString()
            };
        }

        YT_LOG_INFO("Acquired slot for query (SlotIndex: %v, QueryId: %v)", acquiredProcess->SlotIndex, queryId);

        auto getUsedClustersReq = acquiredProcess->PluginProxy.GetUsedClusters();
        getUsedClustersReq->set_query_text(queryText);
        getUsedClustersReq->set_settings(settings.ToString());
        for (const auto& file : files) {
            auto queryFile = getUsedClustersReq->add_files();
            queryFile->set_name(file.Name);
            queryFile->set_content(file.Content);
            queryFile->set_type(static_cast<TYqlQueryFile_EContentType>(file.Type));
        }

        auto response = WaitFor(getUsedClustersReq->Invoke());
        if (!response.IsOK()) {
            YT_LOG_ERROR(response, "Failed to get cluster result from subprocess (QueryId: %v, SlotIndex %v)", queryId, acquiredProcess->SlotIndex);
            return TClustersResult{
                .YsonError = ConvertToYsonString<TError>(
                    TError("Failed to get used clusters result from subprocess")
                    << response
                    << TErrorAttribute("slot_index", acquiredProcess->SlotIndex))
                    .ToString()
            };
        }

        auto responseValue = response.Value();
        TClustersResult result;
        for (const auto& cluster : responseValue->clusters()) {
            result.Clusters.push_back(cluster);
        }
        if (responseValue->has_error()) {
            result.YsonError = responseValue->error();
        }

        return result;
    }

    // Gets an acquired in GetUsedClusters subprocess and routes Run call to it. Marks subprocess
    // as active so it would not reinitialize. Long blocking call
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
        auto pluginProcess = pluginProcessOrError.Value();
        YT_LOG_INFO("Running query in slot (SlotIndex: %v, QueryId: %v)", pluginProcess->SlotIndex, queryId);

        // Mark query as active so it will not be restarted in CheckProcessIsActive.
        pluginProcess->IsActive.store(true);

        // Handle query finish in the end.
        auto finishQueryGuard = Finally(BIND(&TYqlProcessPlugin::OnQueryFinish, this, queryId, pluginProcess)
                                            .Via(Invoker_));

        auto runQueryReq = pluginProcess->PluginProxy.RunQuery();
        ToProto(runQueryReq->mutable_query_id(), queryId);
        runQueryReq->set_user(user);
        runQueryReq->set_credentials(credentials.ToString());
        runQueryReq->set_query_text(queryText);
        runQueryReq->set_settings(settings.ToString());
        for (const auto& file : files) {
            auto queryFile = runQueryReq->add_files();
            queryFile->set_name(file.Name);
            queryFile->set_content(file.Content);
            queryFile->set_type(static_cast<TYqlQueryFile_EContentType>(file.Type));
        }
        runQueryReq->set_mode(executeMode);

        auto response = WaitFor(runQueryReq->Invoke());
        if (!response.IsOK()) {
            YT_LOG_ERROR(response, "Failed to get query result from subprocess (QueryId: %v, SlotIndex %v)", queryId, pluginProcess->SlotIndex);
            return TQueryResult{
                .YsonError = ConvertToYsonString(
                    TError("Failed to get query result from subprocess")
                    << response
                    << TErrorAttribute("slot_index", pluginProcess->SlotIndex))
                    .ToString()
            };
        }

        return ToQueryResult(response.Value()->response());
    }

    virtual TQueryResult GetProgress(TQueryId queryId) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);
        if (!pluginProcessOrError.IsOK()) {
            return TQueryResult{
                .YsonError = ConvertToYsonString<TError>(pluginProcessOrError).ToString()
            };
        }
        auto pluginProcess = pluginProcessOrError.Value();
        YT_LOG_INFO("Getting query progress (SlotIndex: %v, QueryId: %v)", pluginProcess->SlotIndex, queryId);

        auto getProgressReq = pluginProcess->PluginProxy.GetQueryProgress();
        ToProto(getProgressReq->mutable_query_id(), queryId);
        auto response = WaitFor(getProgressReq->Invoke());
        if (!response.IsOK()) {
            YT_LOG_ERROR("Failed to get query progress from subprocess (QueryId: %v, SlotIndex %v)", queryId, pluginProcess->SlotIndex);
            return TQueryResult{
                .YsonError = ConvertToYsonString(
                    TError("Failed to get query progress from subprocess")
                    << response
                    << TErrorAttribute("slot_index", pluginProcess->SlotIndex))
                    .ToString()
            };
        }

        return ToQueryResult(response.Value()->response());
    }

    virtual TAbortResult Abort(TQueryId queryId) override
    {
        auto pluginProcessOrError = GetYqlPluginByQueryId(queryId);
        if (!pluginProcessOrError.IsOK()) {
            return TAbortResult{
                .YsonError = ConvertToYsonString<TError>(pluginProcessOrError).ToString()
            };
        }

        auto pluginProcess = pluginProcessOrError.Value();
        YT_LOG_INFO("Aborting query (SlotIndex: %v, QueryId: %v)", pluginProcess->SlotIndex, queryId);
        auto abortQueryReq = pluginProcess->PluginProxy.AbortQuery();
        ToProto(abortQueryReq->mutable_query_id(), queryId);

        auto response = WaitFor(abortQueryReq->Invoke());
        if (!response.IsOK()) {
            YT_LOG_ERROR(response, "Failed to abort query (QueryId: %v, SlotIndex: %v)", queryId, pluginProcess->SlotIndex);
            return TAbortResult{
                .YsonError = ConvertToYsonString(
                    TError("Failed to abort query")
                    << response
                    << TErrorAttribute("slot_index", pluginProcess->SlotIndex))
                    .ToString()
            };
        }

        TAbortResult abortResult;
        if (response.Value()->has_error()) {
            abortResult.YsonError = response.Value()->error();
        }
        return abortResult;
    }

    virtual void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) override
    {
        YT_LOG_INFO("Updating dynamic config");

        auto guard = NThreading::WriterGuard(ProcessesLock_);
        UpdateConfigTemplateOnConfigChanged(config);
        std::vector<TFuture<void>> futures;
        TString gatewaysConfig = config.GatewaysConfig.ToString();

        for (int i = 0; i < Config_->SlotsCount; ++i) {
            futures.push_back(BIND([gatewaysConfig, process = YqlProcesses_[i]] {
                // process is not initialized yet, it will start with updated config.
                if (!process) {
                    return;
                }
                auto request = process->PluginProxy.OnDynamicConfigChanged();
                request->set_gateways_config(gatewaysConfig);
                WaitFor(request->Invoke()).ThrowOnError();
            })
                .AsyncVia(Invoker_)
                .Run());
        }

        guard.Release();

        auto result = WaitFor(AllSet(futures));
        if (!result.IsOK()) {
            YT_LOG_ERROR(result, "Failed to update dynamic config");
            return;
        }

        YT_LOG_INFO("Dynamic config updated");
    }

    virtual NYTree::IMapNodePtr GetOrchidNode() const override
    {
        auto guard = NThreading::ReaderGuard(ProcessesLock_);
        auto yqlPluginNode = NYTree::GetEphemeralNodeFactory()->CreateMap();
        yqlPluginNode->AddChild("active_processes", NYTree::BuildYsonNodeFluently().Value(RunningYqlQueries_.size()));
        yqlPluginNode->AddChild("standby_processes", NYTree::BuildYsonNodeFluently().Value(StandbyProcessesQueue_.size()));
        yqlPluginNode->AddChild("total_processes", NYTree::BuildYsonNodeFluently().Value(Config_->SlotsCount));
        return yqlPluginNode;
    }

private:
    static TString SocketName;

    TYqlProcessPluginConfigPtr Config_;

    TActionQueuePtr Queue_;
    IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ProcessesLock_);
    THashMap<TQueryId, int> RunningYqlQueries_;
    THashMap<int, TQueryId> YqlQueriesBySlotIndex_;
    std::vector<TYqlPluginRunningProcessPtr> YqlProcesses_;
    std::deque<int> StandbyProcessesQueue_;
    THashSet<int> StandByProcesses_;

    TYqlPluginProcessInternalConfigPtr ConfigTemplate_;

    NProfiling::TGauge StandbyProcessesGauge_;
    NProfiling::TGauge ActiveProcessesGauge_;
    NProfiling::TGauge ProcessesLimitGauge_;

    std::shared_ptr<IRetryPolicy<const std::exception&>> StartPluginRetryPolicy_ = IRetryPolicy<const std::exception&>::GetExponentialBackoffPolicy(
        /*retryClassFunction*/ [](const std::exception&) {
            return ERetryErrorClass::LongRetry;
        },
        /*minDelay*/ TDuration::Seconds(5),
        /*minLongRetryDelay*/ TDuration::Seconds(5),
        /*maxDelay*/ TDuration::Seconds(30),
        /*maxRetries*/ 5);

private:
    TYqlPluginRunningProcessPtr AcquireSlotForQuery(TQueryId queryId) noexcept
    {
        auto guard = NThreading::WriterGuard(ProcessesLock_);

        if (StandbyProcessesQueue_.empty()) {
            YT_LOG_ERROR("No standby slot to acquire for query (QueryId: %v, ActiveProcesses: %v)", queryId, RunningYqlQueries_.size());
            return nullptr;
        }

        int acquiredSlot = StandbyProcessesQueue_.front();

        StandbyProcessesQueue_.pop_front();
        StandByProcesses_.erase(acquiredSlot);

        RunningYqlQueries_[queryId] = acquiredSlot;
        YqlQueriesBySlotIndex_[acquiredSlot] = queryId;

        StandbyProcessesGauge_.Update(StandbyProcessesQueue_.size());
        ActiveProcessesGauge_.Update(RunningYqlQueries_.size());

        auto acquiredProcess = YqlProcesses_[acquiredSlot];

        // We should check whether query was started in process. If it did not, then
        // we need to restart process, because otherwise it will be stuck forever.
        NYT::NConcurrency::TDelayedExecutor::Submit(
            BIND(&TYqlProcessPlugin::CheckProcessIsActive, this, acquiredProcess, queryId),
            Config_->CheckProcessActiveDelay,
            Invoker_);

        return acquiredProcess;
    }

    TErrorOr<TYqlPluginRunningProcessPtr> GetYqlPluginByQueryId(TQueryId queryId)
    {
        auto guard = NThreading::ReaderGuard(ProcessesLock_);

        if (!RunningYqlQueries_.contains(queryId)) {
            YT_LOG_WARNING("Query was not found in running queries (QueryId: %v)", queryId);
            return TError("Query %v was not found in running queries", queryId);
        }

        int slotIndex = RunningYqlQueries_[queryId];

        return YqlProcesses_[slotIndex];
    }

    void ClearSlot(int slotIndex)
    {
        auto guard = NThreading::WriterGuard(ProcessesLock_);
        // Handle a case when standby process finished unexpectedly, should happen rarely.
        if (StandByProcesses_.contains(slotIndex)) {
            StandbyProcessesQueue_.erase(std::find(StandbyProcessesQueue_.begin(), StandbyProcessesQueue_.end(), slotIndex));
            StandByProcesses_.erase(slotIndex);
        }
        if (YqlQueriesBySlotIndex_.contains(slotIndex)) {
            RunningYqlQueries_.erase(YqlQueriesBySlotIndex_[slotIndex]);
            YqlQueriesBySlotIndex_.erase(slotIndex);
        }
        ActiveProcessesGauge_.Update(RunningYqlQueries_.size());
        StandbyProcessesGauge_.Update(StandbyProcessesQueue_.size());
    }

    void CheckProcessIsActive(TYqlPluginRunningProcessPtr process, TGuid queryId)
    {
        if (process->IsActive.load()) {
            return;
        }
        YT_LOG_WARNING("Query did not start after acquiring process; Restarting process (SlotIndex: %v, QueryId: %v)", process->SlotIndex, queryId);
        StopRunningProcess(process);
    }

    void StartPluginInProcess(TYqlPluginRunningProcessPtr process)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        int slotIndex = process->SlotIndex;
        YT_LOG_INFO("Starting plugin in process (SlotIndex: %v)", slotIndex);

        // Here we are waiting for rpc server inside started subprocess to be ready to accept calls.
        bool success = DoWithRetry<std::exception>(
            BIND(&TYqlProcessPlugin::DoStartPluginInProcess, this, process),
            StartPluginRetryPolicy_,
            false,
            [slotIndex = process->SlotIndex](const std::exception& exception) {
                YT_LOG_WARNING(exception, "Failed to start yql plugin, retrying (SlotIndex: %v)", slotIndex);
            });

        if (!success) {
            YT_LOG_ERROR("All attempts to start plugin in process failed, restarting process (SlotIndex: %v)", slotIndex);
            StopRunningProcess(process);
            Invoker_->Invoke(BIND(&TYqlProcessPlugin::StartPluginProcess, this, slotIndex));
            return;
        }

        auto guard = NYT::NThreading::WriterGuard(ProcessesLock_);

        StandbyProcessesQueue_.push_back(slotIndex);
        StandByProcesses_.insert(slotIndex);
        StandbyProcessesGauge_.Update(StandbyProcessesQueue_.size());

        process->Result.Subscribe(
            BIND([slotIndex, this](const TErrorOr<void> result) {
                YT_LOG_DEBUG(result, "Process finished (SlotIndex: %v)", slotIndex);
                ClearSlot(slotIndex);
                StartPluginProcess(slotIndex);
            }).Via(Invoker_));

        YT_LOG_INFO("Successfully started plugin in subprocess (SlotIndex: %v)", slotIndex);
    }

    void DoStartPluginInProcess(TYqlPluginRunningProcessPtr process)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Starting plugin in process (SlotIndex: %v)", process->SlotIndex);
        Y_ENSURE(
            NFS::Exists(GetUnixDomainSocketPath(process->SlotIndex)),
            "Unix socket in slot must exist before calling Start method");
        WaitFor(process->PluginProxy.Start()->Invoke()).ThrowOnError();
    }

    void OnQueryFinish(TQueryId queryId, TYqlPluginRunningProcessPtr process)
    {
        YT_LOG_DEBUG("Query finished, clearing slot and restarting process (QueryId: %v, SlotIndex: %v)", queryId, process->SlotIndex);
        ClearSlot(process->SlotIndex);
        Invoker_->Invoke(BIND(&TYqlProcessPlugin::StopRunningProcess, this, process));
    }

    void StopRunningProcess(TYqlPluginRunningProcessPtr process)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        int slotIndex = process->SlotIndex;

        YT_LOG_DEBUG("Stopping process (SlotIndex: %v)", slotIndex);
        auto guard = NThreading::WriterGuard(ProcessesLock_);

        process->Process->Kill(SIGKILL);
        if (process == YqlProcesses_[slotIndex]) {
            YqlProcesses_[slotIndex] = nullptr;
        }
    }

    void InitializeProcessPool()
    {
        YT_LOG_INFO("Initializing process pool");
        for (int i = 0; i < Config_->SlotsCount; ++i) {
            Invoker_->Invoke(BIND_NO_PROPAGATE([this, i]() {
                StartProcess(i);
            }));
        }
    }

    void StartPluginProcess(int slotIndex)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_LOG_INFO("Starting yql plugin process (SlotIndex: %v)", slotIndex);

        BIND_NO_PROPAGATE(&TYqlProcessPlugin::StartProcess, this, slotIndex)
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(BIND_NO_PROPAGATE([this, slotIndex](const TErrorOr<TYqlPluginRunningProcessPtr>& result) {
                if (!result.IsOK()) {
                    YT_LOG_WARNING(result, "Failed to start yql plugin process, trying again (SlotIndex: %v)", slotIndex);
                    Invoker_->Invoke(BIND_NO_PROPAGATE(&TYqlProcessPlugin::StartPluginProcess, this, slotIndex));
                    return;
                }
                YT_LOG_DEBUG("Successfully started process, starting yql plugin in process (SlotIndex: %v)", slotIndex);
                Invoker_->Invoke(BIND_NO_PROPAGATE(&TYqlProcessPlugin::StartPluginInProcess, this, result.Value()));
            }));
    }

    TYqlPluginRunningProcessPtr StartProcess(int slotIndex)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_LOG_INFO("Starting plugin process (SlotIndex: %v)", slotIndex);
        auto guard = NThreading::WriterGuard(ProcessesLock_);
        return YqlProcesses_[slotIndex] = DoStartProcess(slotIndex);
    }

    TString GetSlotPath(int slotIndex) const
    {
        return NFS::CombinePaths({Config_->SlotsRootPath, Format("%v", slotIndex)});
    }

    TString GetUnixDomainSocketPath(int slotIndex) const
    {
        return NFS::CombinePaths(
            GetSlotPath(slotIndex),
            SocketName);
    }

    TYqlPluginRunningProcessPtr DoStartProcess(int slotIndex)
    {
        TString workingDirectory = GetSlotPath(slotIndex);
        NFS::MakeDirRecursive(workingDirectory, MODE0711);
        TString unixDomainSocketPath = GetUnixDomainSocketPath(slotIndex);

        if (NFS::Exists(unixDomainSocketPath)) {
            NFS::Remove(unixDomainSocketPath);
        }

        auto serverConfig = NYT::NBus::TBusServerConfig::CreateUds(unixDomainSocketPath);
        auto clientConfig = NYT::NBus::TBusClientConfig::CreateUds(unixDomainSocketPath);

        TYqlPluginProcessInternalConfigPtr config = BuildProcessConfig(slotIndex, serverConfig);
        auto client = NBus::CreateBusClient(clientConfig);

        TProcessBasePtr process = New<TSimpleProcess>(YqlAgentProgrammName);

        WriteConfig(config, NFS::CombinePaths(workingDirectory, YqlPluginConfigFilename));

        process->SetWorkingDirectory(workingDirectory);
        process->AddArguments({
            YqlPluginSubcommandName, 
            "--config", YqlPluginConfigFilename
        });

        return New<TYqlPluginRunningProcess>(
            slotIndex,
            config,
            process,
            process->Spawn(),
            TYqlPluginProxy(NRpc::NBus::CreateBusChannel(client)));
    }

    TYqlPluginProcessInternalConfigPtr BuildProcessConfig(int slotIndex, NBus::TBusServerConfigPtr serverConfig)
    {
        TYqlPluginProcessInternalConfigPtr config = NYTree::CloneYsonStruct(ConfigTemplate_);

        config->SlotIndex = slotIndex;
        config->BusServer = serverConfig;

        auto fileStorageConfig = NYT::NodeFromYsonString(config->PluginOptions->FileStorageConfig.ToString());
        fileStorageConfig.AsMap()["path"] = NFS::CombinePaths(GetSlotPath(slotIndex), "tmp");
        config->PluginOptions->FileStorageConfig = NYson::TYsonString(NYT::NodeToYsonString(fileStorageConfig));

        auto logManagerConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
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

        return config;
    }

    void WriteConfig(TYqlPluginProcessInternalConfigPtr config, TString configPath)
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

        auto dynamicGatewaysConfig = NYT::NodeFromYsonString(config.GatewaysConfig.AsStringBuf());
        auto templateGatewaysConfig = NYT::NodeFromYsonString(ConfigTemplate_->PluginOptions->GatewayConfig.AsStringBuf());

        NYT::MergeNodes(templateGatewaysConfig, dynamicGatewaysConfig);

        ConfigTemplate_->PluginOptions->GatewayConfig = NYT::NYson::TYsonString(NYT::NodeToYsonString(templateGatewaysConfig));
    }

    static TYqlPluginProcessInternalConfigPtr BuildPluginConfigTemplate(
        TYqlProcessPluginConfigPtr config,
        TYqlPluginOptions options,
        NYqlAgent::TBootstrap* bootstrap)
    {
        TYqlPluginProcessInternalConfigPtr result = New<TYqlPluginProcessInternalConfig>();

        result->SetSingletonConfig(config->LogManagerTemplate);
        result->ClusterConnection = bootstrap->GetNativeServerBootstrapConfig()->ClusterConnection->Clone();

        result->PluginOptions = New<TYqlProcessPluginOptions>();
        result->PluginOptions->GatewayConfig = options.GatewayConfig;
        if (options.DqGatewayConfig) {
            result->PluginOptions->DqGatewayConfig = options.DqGatewayConfig;
        }
        if (options.DqManagerConfig) {
            result->PluginOptions->DqManagerConfig = options.DqManagerConfig;
        }
        result->PluginOptions->FileStorageConfig = options.FileStorageConfig;
        result->PluginOptions->SingletonsConfig = options.SingletonsConfig;
        result->PluginOptions->YqlPluginSharedLibrary = options.YqlPluginSharedLibrary;
        result->PluginOptions->Libraries = options.Libraries;
        result->PluginOptions->OperationAttributes = options.OperationAttributes;
        result->PluginOptions->YTTokenPath = options.YTTokenPath;

        return result;
    }

    TQueryResult ToQueryResult(const TYqlResponse& yqlResponse) const
    {
        TQueryResult result;

        SetQueryResultField(result.YsonResult, yqlResponse, &TYqlResponse::has_result, &TYqlResponse::result);
        SetQueryResultField(result.Plan, yqlResponse, &TYqlResponse::has_plan, &TYqlResponse::plan);
        SetQueryResultField(result.Progress, yqlResponse, &TYqlResponse::has_progress, &TYqlResponse::progress);
        SetQueryResultField(result.Statistics, yqlResponse, &TYqlResponse::has_statistics, &TYqlResponse::statistics);
        SetQueryResultField(result.YsonError, yqlResponse, &TYqlResponse::has_error, &TYqlResponse::error);
        SetQueryResultField(result.TaskInfo, yqlResponse, &TYqlResponse::has_task_info, &TYqlResponse::task_info);

        return result;
    }

    void SetQueryResultField(
        std::optional<TString>& queryResultField,
        const TYqlResponse& response,
        std::function<bool(const TYqlResponse*)> isFieldPresent,
        std::function<TString(const TYqlResponse*)> fieldValue) const
    {
        if (isFieldPresent(&response)) {
            queryResultField = fieldValue(&response);
        }
    }
};

TString TYqlProcessPlugin::SocketName = "yql-plugin.sock";

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateProcessYqlPlugin(
    NYqlAgent::TBootstrap* bootstrap,
    TYqlPluginOptions options,
    NYqlPlugin::NProcess::TYqlProcessPluginConfigPtr config,
    const NProfiling::TProfiler& profiler)
{
    return std::make_unique<TYqlProcessPlugin>(bootstrap, std::move(options), config, profiler);
}

} // namespace NProcess
} // namespace NYT::NYqlPlugin
