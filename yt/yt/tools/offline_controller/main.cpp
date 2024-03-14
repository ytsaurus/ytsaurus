#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/private.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/operation_controller.h>
#include <yt/yt/server/controller_agent/operation_controller_host.h>

#include <yt/yt/server/controller_agent/controllers/operation_controller_detail.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/logging/log_manager.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/cast.h>
#include <util/system/env.h>

using namespace NLastGetopt;
using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NChunkClient;
using namespace NYT::NConcurrency;
using namespace NYT::NControllerAgent;
using namespace NYT::NEventLog;
using namespace NYT::NScheduler;
using namespace NYT::NCoreDump;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDummyEventLogWriter
    : public IEventLogWriter
{
public:
    std::unique_ptr<NYT::NYson::IYsonConsumer> CreateConsumer() override
    {
        return nullptr;
    }

    TEventLogManagerConfigPtr GetConfig() const override
    {
        YT_UNIMPLEMENTED();
    }

    void UpdateConfig(const TEventLogManagerConfigPtr&) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> Close() override
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDummyOperationControllerHost
    : public IOperationControllerHost
{
public:
    TDummyOperationControllerHost()
        : ActionQueue_(New<TActionQueue>("DummyOperationControllerHost"))
        , EventLogWriter_(New<TDummyEventLogWriter>())
        , MediumDirectory_(New<TMediumDirectory>())
    { }

    void Disconnect(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

    const TJobTrackerOperationHandlerPtr& GetJobTrackerOperationHandler() const override
    {
        YT_UNIMPLEMENTED();
    }

    void InterruptJob(
        TJobId /*jobId*/,
        EInterruptReason /*reason*/,
        TDuration /*timeout*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void RequestJobGracefulAbort(TJobId /*jobId*/, EAbortReason /*reason*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void RegisterJob(TStartedJobInfo) override
    {
        YT_UNIMPLEMENTED();
    }

    void ReviveJobs(std::vector<TStartedJobInfo>) override
    {
        YT_UNIMPLEMENTED();
    }

    void ReleaseJobs(std::vector<NControllerAgent::TJobToRelease>) override
    {
        YT_UNIMPLEMENTED();
    }

    void AbortJob(
        TJobId /*jobId*/,
        NScheduler::EAbortReason /*abortReason*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TOperationSnapshot> DownloadSnapshot() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> RemoveSnapshot() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> FlushOperationNode() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> UpdateInitializedOperationNode(bool /*isCleanOperationStart*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> UpdateControllerFeatures(const NYson::TYsonString& /*featureYson*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> AttachChunkTreesToLivePreview(
        NTransactionClient::TTransactionId,
        NCypressClient::TNodeId,
        const std::vector<NChunkClient::TChunkTreeId>&) override
    {
        YT_UNIMPLEMENTED();
    }

    void AddChunkTreesToUnstageList(const std::vector<NChunkClient::TChunkId>&, bool) override
    {
        YT_UNIMPLEMENTED();
    }

    const NApi::NNative::IClientPtr& GetClient() override
    {
        YT_UNIMPLEMENTED();
    }

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() override
    {
        YT_UNIMPLEMENTED();
    }

    const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() override
    {
        YT_UNIMPLEMENTED();
    }

    const IInvokerPtr& GetControllerThreadPoolInvoker() override
    {
        return ActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetJobSpecBuildPoolInvoker() override
    {
        return ActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetExecNodesUpdateInvoker() override
    {
        return ActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetConnectionInvoker() override
    {
        return ActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetStatisticsOffloadInvoker() override
    {
        return ActionQueue_->GetInvoker();
    }

    const IEventLogWriterPtr& GetEventLogWriter() override
    {
        return EventLogWriter_;
    }

    const ICoreDumperPtr& GetCoreDumper() override
    {
        YT_UNIMPLEMENTED();
    }

    const TAsyncSemaphorePtr& GetCoreSemaphore() override
    {
        YT_UNIMPLEMENTED();
    }

    const IThroughputThrottlerPtr& GetJobSpecSliceThrottler() override
    {
        YT_UNIMPLEMENTED();
    }

    TJobProfiler* GetJobProfiler() const override
    {
        return nullptr;
    }

    int GetAvailableExecNodeCount() override
    {
        return 0;
    }

    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter&, bool) override
    {
        return New<TRefCountedExecNodeDescriptorMap>();
    }

    TJobResources GetMaxAvailableResources(const NScheduler::TSchedulingTagFilter&) override
    {
        return TJobResources{};
    }

    TInstant GetConnectionTime() override
    {
        YT_UNIMPLEMENTED();
    }

    NScheduler::TIncarnationId GetIncarnationId() override
    {
        YT_UNIMPLEMENTED();
    }

    void OnOperationCompleted() override
    {
        YT_UNIMPLEMENTED();
    }

    void OnOperationAborted(const TError&) override
    {
        YT_UNIMPLEMENTED();
    }

    void OnOperationFailed(const TError&) override
    {
        YT_UNIMPLEMENTED();
    }

    void OnOperationSuspended(const TError&) override
    {
        YT_UNIMPLEMENTED();
    }

    void OnOperationBannedInTentativeTree(const TString&, const std::vector<TAllocationId>&) override
    {
        YT_UNIMPLEMENTED();
    }

    void ValidateOperationAccess(const TString&, NYTree::EPermission) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> UpdateAccountResourceUsageLease(
        NSecurityClient::TAccountResourceUsageLeaseId,
        const NScheduler::TDiskQuota&) override
    {
        YT_UNIMPLEMENTED();
    }

    const TJobReporterPtr& GetJobReporter() override
    {
        YT_UNIMPLEMENTED();
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() override
    {
        return MediumDirectory_;
    }

    std::optional<TJobMonitoringDescriptor> TryAcquireJobMonitoringDescriptor(TOperationId /*operationId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    bool ReleaseJobMonitoringDescriptor(TOperationId /*operationId*/, TJobMonitoringDescriptor /*descriptor*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void UpdateRunningAllocationsStatistics(std::vector<TAgentToSchedulerRunningAllocationStatistics> /*runningAllocationStatisticsUpdates*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    TActionQueuePtr ActionQueue_;
    IEventLogWriterPtr EventLogWriter_;
    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

struct TOfflineOperation
{
    EOperationType Type;
    TString Spec;
    int SnapshotVersion;
    std::vector<TSharedRef> Snapshot;

    constexpr static TStringBuf OperationTypeFileNameSuffix = ".type";
    constexpr static TStringBuf SpecFileNameSuffix = ".spec";
    constexpr static TStringBuf SnapshotVersionFileNameSuffix = ".version";
    constexpr static TStringBuf SnapshotFileNameSuffix = ".snapshot";
};

TOfflineOperation LoadFromFiles(const TString& baseFileName)
{
    TOfflineOperation offlineOperation;
    offlineOperation.Type = ConvertTo<EOperationType>(TIFStream(baseFileName + TOfflineOperation::OperationTypeFileNameSuffix).ReadAll());
    offlineOperation.Spec = TIFStream(baseFileName + TOfflineOperation::SpecFileNameSuffix).ReadAll();
    offlineOperation.SnapshotVersion = FromString<int>(TIFStream(baseFileName + TOfflineOperation::SnapshotVersionFileNameSuffix).ReadAll());
    offlineOperation.Snapshot.push_back(
        TSharedRef::FromString(TIFStream(baseFileName + TOfflineOperation::SnapshotFileNameSuffix).ReadAll()));
    return offlineOperation;
}

void StoreToFiles(const TOfflineOperation& offlineOperation, const TString& baseFileName)
{
    TOFStream(baseFileName + TOfflineOperation::OperationTypeFileNameSuffix).Write(ConvertTo<TString>(offlineOperation.Type));
    TOFStream(baseFileName + TOfflineOperation::SpecFileNameSuffix).Write(offlineOperation.Spec);
    TOFStream(baseFileName + TOfflineOperation::SnapshotVersionFileNameSuffix).Write(ToString(offlineOperation.SnapshotVersion));

    TOFStream snapshotStream(baseFileName + TOfflineOperation::SnapshotFileNameSuffix);
    for (const auto& block : offlineOperation.Snapshot) {
        snapshotStream.Write(TStringBuf(block.Begin(), block.End()));
    }
}

IOperationControllerPtr CreateOperationController(const TOfflineOperation& offlineOperation)
{
    NControllerAgent::NProto::TOperationDescriptor operationDescriptor;
    operationDescriptor.set_spec(offlineOperation.Spec);
    operationDescriptor.set_acl("[]");
    operationDescriptor.set_operation_type(static_cast<int>(offlineOperation.Type));

    auto operation = New<TOperation>(operationDescriptor);

    auto operationControllerHost = New<TDummyOperationControllerHost>();
    operation->SetHost(operationControllerHost);

    auto controllerAgentFakeConfig = New<TControllerAgentConfig>();
    auto operationController = CreateControllerForOperation(controllerAgentFakeConfig, operation.Get());

    TOperationSnapshot operationSnapshot;
    operationSnapshot.Version = offlineOperation.SnapshotVersion;
    operationSnapshot.Blocks = offlineOperation.Snapshot;

    operationController->LoadSnapshot(operationSnapshot);

    return operationController;
}

TOfflineOperation DownloadOperation(const TString& token, const TString& proxy, const TString& path)
{
    auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = proxy;
    auto connection = CreateConnection(connectionConfig);

    auto clientOptions = NApi::TClientOptions::FromToken(token);
    auto client = connection->CreateClient(clientOptions);

    TOfflineOperation operation;

    NApi::TGetNodeOptions options;
    options.Attributes = {"full_spec", "operation_type"};
    auto node = ConvertToNode(WaitFor(client->GetNode(path, options)).ValueOrThrow());
    const auto& attributes = node->Attributes();
    operation.Spec = attributes.FindYson("full_spec").ToString();
    operation.Type = ConvertTo<EOperationType>(attributes.Get<TString>("operation_type"));

    operation.SnapshotVersion = ConvertToNode(WaitFor(client->GetNode(path + "/snapshot/@version")).ValueOrThrow())->AsInt64()->GetValue();

    auto fileReader = WaitFor(client->CreateFileReader(path + "/snapshot")).ValueOrThrow();
    while (true) {
        auto block = WaitFor(fileReader->Read()).ValueOrThrow();
        if (!block) {
            break;
        }
        operation.Snapshot.emplace_back(block);
    }

    return operation;
}

////////////////////////////////////////////////////////////////////////////////

void GuardedMain(int argc, char** argv)
{
    auto mode = TString("load");
    TString loadFromFiles;
    TString storeToFiles;
    TString proxy;
    TString path;
    TString operationId;
    TString token;
    bool loop = false;
    bool rct = false;
    bool ignoreVersionMismatch = false;
    bool prepareAndExit = false;

    TOpts opts;
    opts.AddLongOption("mode", "Offline controller mode (load)")
        .StoreResult(&mode);
    opts.AddLongOption("load-from-files", "Prefix of files names to load operation snapshot from")
        .StoreResult(&loadFromFiles);
    opts.AddLongOption("store-to-files", "Prefix of files names to store operation snapshot to")
        .StoreResult(&storeToFiles);
    opts.AddLongOption("rct", "Print RCT output")
        .StoreTrue(&rct);
    opts.AddLongOption("loop", "Run operation controller in infinite loop")
        .StoreTrue(&loop);
    opts.AddLongOption("proxy", "YT cluster")
        .StoreResult(&proxy);
    opts.AddLongOption("path", "Operation path in Cypress")
        .StoreResult(&path);
    opts.AddLongOption("operation-id", "Operation id")
        .StoreResult(&operationId);
    opts.AddLongOption("token", "YT token")
        .StoreResult(&token);
    opts.AddLongOption("ignore-version-mismatch", "Don't throw and quietly exit upon snapshot version mismatch")
        .StoreTrue(&ignoreVersionMismatch);
    opts.AddLongOption("prepare-and-exit", "Only prepare for validation")
        .StoreTrue(&prepareAndExit);

    TOptsParseResult results(&opts, argc, argv);

    if (!token) {
        auto tokenPath = NFS::GetHomePath() + "/.yt/token";
        if (NFS::Exists(tokenPath)) {
            TIFStream tokenStream(tokenPath);
            tokenStream >> token;
        } else {
            token = GetEnv("YT_TOKEN");
        }
    }

    if (!proxy) {
        proxy = GetEnv("YT_PROXY");
    }

    if (!path && operationId) {
        path = Format("//sys/operations/%v/%v", operationId.substr(operationId.size() - 2), operationId);
    }

    NLogging::TLogManager::Get()->Configure(NLogging::TLogManagerConfig::CreateQuiet());

    TOfflineOperation operation;
    if (loadFromFiles) {
        operation = LoadFromFiles(loadFromFiles);
    } else {
        operation = DownloadOperation(token, proxy, path);
    }

    NBus::TTcpDispatcher::Get()->DisableNetworking();

    if (!ValidateSnapshotVersion(operation.SnapshotVersion)) {
        auto error = TError("Snapshot version %v is not supported over current snapshot version %v (%v)",
            operation.SnapshotVersion,
            ToUnderlying(GetCurrentSnapshotVersion()),
            GetCurrentSnapshotVersion());
        if (ignoreVersionMismatch) {
            Cerr << error.GetMessage() << Endl;
            return;
        }
        THROW_ERROR_EXCEPTION(error);
    }

    if (storeToFiles) {
        StoreToFiles(operation, storeToFiles);
    }

    if (prepareAndExit) {
        return;
    }

    auto controller = CreateOperationController(operation);

    if (rct) {
        Cerr << TRefCountedTracker::Get()->GetDebugInfo(/*sortByColumn=*/2/*bytesAlive*/) << Endl;
    }

    if (loop) {
        while (true) { }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    try {
        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cerr << ToString(TError(ex)) << Endl;
        return 1;
    }
}

////////////////////////////////////////////////////////////////////////////////
