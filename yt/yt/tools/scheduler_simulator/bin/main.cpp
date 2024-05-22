#include "private.h"
#include "config.h"
#include "operation.h"
#include "operation_controller.h"
#include "operation_description.h"
#include "scheduler_strategy_host.h"
#include "shared_data.h"
#include "node_shard.h"
#include "control_thread.h"

#include <yt/yt/server/scheduler/public.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>

#include <yt/yt/core/logging/public.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/yson/lexer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/stream.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <util/system/fs.h>

namespace NYT {

using namespace NSchedulerSimulator;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;
using namespace NPhoenix;
using namespace NJobTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

TJobResources GetNodeResourceLimit(const TNodeResourcesConfigPtr& config)
{
    TJobResources resourceLimits;
    resourceLimits.SetMemory(config->Memory);
    resourceLimits.SetCpu(TCpuResource(config->Cpu));
    resourceLimits.SetUserSlots(config->UserSlots);
    resourceLimits.SetNetwork(config->Network);
    return resourceLimits;
}

std::vector<TExecNodePtr> CreateExecNodes(const std::vector<TNodeGroupConfigPtr>& nodeGroups)
{
    std::vector<TExecNodePtr> execNodes;

    auto diskResources = TDiskResources{
        .DiskLocationResources = {
            TDiskResources::TDiskLocationResources{
                .Usage = 0,
                .Limit = 100_GB,
            },
        },
    };

    for (const auto& nodeGroupConfig : nodeGroups) {
        for (int i = 0; i < nodeGroupConfig->Count; ++i) {
            // NB: 0 is InvalidNodeId therefore we need +1.
            auto nodeId = TNodeId(execNodes.size() + 1);
            TNodeDescriptor descriptor("node" + ToString(nodeId));

            auto node = New<TExecNode>(nodeId, descriptor, NScheduler::ENodeState::Online);
            node->SetTags(TBooleanFormulaTags(nodeGroupConfig->Tags));
            node->SetResourceLimits(GetNodeResourceLimit(nodeGroupConfig->ResourceLimits));
            node->SetDiskResources(diskResources);
            node->SetMasterState(NNodeTrackerClient::ENodeState::Online);
            node->SetSchedulerState(NScheduler::ENodeState::Online);
            execNodes.push_back(node);
        }
    }

    return execNodes;
}

std::vector<TExecNodePtr> CreateExecNodesFromNode(const INodePtr& nodeGroupsNode)
{
    std::vector<TNodeGroupConfigPtr> nodeGroups;
    Deserialize(nodeGroups, nodeGroupsNode);
    return CreateExecNodes(nodeGroups);
}

std::vector<TExecNodePtr> CreateExecNodesFromFile(const TString& nodeGroupsFilename)
{
    try {
        TIFStream configStream(nodeGroupsFilename);
        return CreateExecNodesFromNode(ConvertToNode(&configStream));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading node groups") << ex;
    }
}

TSchedulerConfigPtr LoadSchedulerConfigFromNode(const INodePtr& schedulerConfigNode)
{
    TSchedulerConfigPtr schedulerConfig;
    Deserialize(schedulerConfig, schedulerConfigNode);
    return schedulerConfig;
}

TSchedulerConfigPtr LoadSchedulerConfigFromFile(const TString& schedulerConfigFilename)
{
    try {
        TIFStream configStream(schedulerConfigFilename);
        return LoadSchedulerConfigFromNode(ConvertToNode(&configStream));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading scheduler config") << ex;
    }
}

TInstant FindEarliestTime(const std::vector<TOperationDescription>& operations)
{
    auto earliestTime = TInstant::Max();
    for (const auto& operation : operations) {
        earliestTime = std::min(earliestTime, operation.StartTime);
    }
    return earliestTime;
}

std::vector<TOperationDescription> LoadOperations(bool shiftOperationsToStart)
{
    std::vector<TOperationDescription> operations;
    {
        TStreamLoadContext context(&Cin);
        Load(context, operations);
    }
    if (shiftOperationsToStart) {
        const auto earliestTime = FindEarliestTime(operations);
        for (auto& operation : operations) {
            operation.StartTime = earliestTime;
        }
    }
    return operations;
}

TYsonString LoadPoolTreesYson(const TString& poolTreesFilename)
{
    try {
        TIFStream configStream(poolTreesFilename);
        return ConvertToYsonString(&configStream);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading pool trees") << ex;
    }
}

template <class T>
class TYsonListExtractor
    : public TForwardingYsonConsumer
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, ExtractedCount);

public:
    TYsonListExtractor(const std::function<void(const T&)>& onEntryExtracted, TLogger logger)
        : ExtractedCount_(0)
        , OnEntryExtracted_(onEntryExtracted)
        , Logger(logger)
    { }

    void OnMyListItem() override
    {
        if (Builder_) {
            ExtractEntry();
        }
        Builder_ = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        Builder_->BeginTree();
        Forward(Builder_.get());
    }

    void Finish()
    {
        if (Builder_) {
            ExtractEntry();
            Builder_.reset();
        }
    }

private:
    void ExtractEntry()
    {
        auto node = Builder_->EndTree();
        OnEntryExtracted_(ConvertTo<T>(node));
        ++ExtractedCount_;
        if (ExtractedCount_ % 1000 == 0) {
            YT_LOG_INFO("Records extracted: %v", ExtractedCount_);
        }
    }

    std::function<void(const T&)> OnEntryExtracted_;
    std::unique_ptr<ITreeBuilder> Builder_;

    TLogger Logger;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSchedulerSimulatorProgram
    : public TProgram
    , public TProgramPdeathsigMixin
{
public:
    TSchedulerSimulatorProgram()
        : TProgramPdeathsigMixin(Opts_)
    {
        Opts_.AddLongOption("allow-debug-mode", "allow running simulator in debug mode")
            .NoArgument()
            .StoreTrue(&AllowDebugMode_);
        Opts_.SetFreeArgsNum(1);
        Opts_.SetFreeArgTitle(0, "SIMULATOR_CONFIG_FILENAME");
        SetCrashOnError();
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        // NB(eshcherbin): It usually doesn't make much sense running the simulator built in debug mode
        // but this occasionally still happens by mistake. Thus we now immediately crash unless debug
        // mode has been explicitly allowed.
#ifndef NDEBUG
        YT_LOG_FATAL_IF(
            !AllowDebugMode_,
            "Running the simulator in debug mode is forbidden by default. Use '--allow-debug-mode' to allow it explicitly.");
#endif

        // TODO(antonkikh): Which of these are actually needed?
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();

        if (HandlePdeathsigOptions()) {
            return;
        }

        auto config = LoadConfig<TSchedulerSimulatorConfig>(/* configFilename */ parseResult.GetFreeArgs()[0]);

        ConfigureSingletons(config);
        StartDiagnosticDump(config);

        {
            auto httpServer = NHttp::CreateServer(config->CreateMonitoringHttpServerConfig());

            NMonitoring::TMonitoringManagerPtr monitoringManager;
            NYTree::IMapNodePtr orchidRoot;
            NMonitoring::Initialize(
                httpServer,
                config->SolomonExporter,
                &monitoringManager,
                &orchidRoot);

            YT_LOG_INFO("Listening for HTTP requests on port %v", httpServer->GetAddress().GetPort());
            httpServer->Start();

            RunSimulation(config);

            httpServer->Stop();
        }
    }

private:
    bool AllowDebugMode_ = false;

    TLogger Logger = SchedulerSimulatorLogger();


    void RunSimulation(const TSchedulerSimulatorConfigPtr& config)
    {
        YT_LOG_INFO("Reading operations description");

        std::vector<TExecNodePtr> execNodes = CreateExecNodesFromFile(config->NodeGroupsFilename);

        YT_LOG_INFO("Discovered %v nodes", execNodes.size());

        YT_VERIFY(!execNodes.empty());

        const auto operations = LoadOperations(config->ShiftOperationsToStart);
        const TInstant earliestTime = FindEarliestTime(operations);

        TFixedBufferFileOutput eventLogOutputStream(config->EventLogFilename);

        auto schedulerConfig = LoadSchedulerConfigFromFile(config->SchedulerConfigFilename);
        auto poolTreesYson = LoadPoolTreesYson(config->PoolTreesFilename);

        TSharedOperationStatisticsOutput statisticsOutput(config->OperationsStatsFilename);

        auto simulatorControlThread = New<TSimulatorControlThread>(
            &execNodes,
            &eventLogOutputStream,
            &statisticsOutput,
            config,
            schedulerConfig,
            operations,
            earliestTime);

        simulatorControlThread->Initialize(poolTreesYson);
        WaitFor(simulatorControlThread->AsyncRun())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////
class TConvertOperationsToBinaryFormatProgram
    : public TProgram
{
public:
    TConvertOperationsToBinaryFormatProgram()
    {
        Opts_.AddLongOption("destination", "path to save converted operations")
            .StoreResult(&Destination_)
            .Required();
    }

private:
    TString Destination_;

    TLogger Logger = TLogger("Converter");

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        TString destinationTemp(Destination_ + ".tmp");

        {
            auto input = TYsonInput(&Cin, NYT::NYson::EYsonType::ListFragment);
            TUnbufferedFileOutput outputTemp(destinationTemp);
            TStreamSaveContext context(&outputTemp);
            TYsonListExtractor<TOperationDescription> extractor(
                [&] (const TOperationDescription& entry) { Save(context, entry); },
                Logger);

            Serialize(input, &extractor);
            extractor.Finish();

            int extractedCount = extractor.GetExtractedCount();
            TUnbufferedFileOutput output(Destination_);
            output.Write(&extractedCount, sizeof extractedCount);

            context.Finish();
        }
        NFs::Cat(Destination_.data(), destinationTemp.data());
        NFs::Remove(destinationTemp.data());
    }

};

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    if (TString(argv[1]) == "convert-operations-to-binary-format") {
        return NYT::TConvertOperationsToBinaryFormatProgram().Run(argc - 1, argv + 1);
    }
    return NYT::TSchedulerSimulatorProgram().Run(argc, argv);
}
