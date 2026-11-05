#include "slot.h"

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_environment.h>
#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/slot_location.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>
#include <yt/yt/server/node/exec_node/volume.h>
#include <yt/yt/server/node/exec_node/volume_manager.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

static NLogging::TLogger SlotLogger("Slot");

using namespace NBus;
using namespace NBus::NTcp;
using namespace NConcurrency;
using namespace NContainers;
using namespace NNode;
using namespace NScheduler;
using namespace NTools;
using namespace NYTree;

using NNet::TIP6Address;

////////////////////////////////////////////////////////////////////////////////

TUserSlotBase::TUserSlotBase(TUserSlotContext context)
    : JobEnvironment_(std::move(context.Environment))
    , Location_(std::move(context.Location))
    , VolumeManager_(std::move(context.VolumeManager))
    , Bootstrap_(context.Bootstrap)
    , SlotGuard_(context.SlotManager->AcquireSlot(context.SlotType, context.RequestedCpu, context.NumaNodeAffinity))
    , SlotIndex_(SlotGuard_->GetSlotIndex())
    , NodeTag_(std::move(context.NodeTag))
    , JobProxyUnixDomainSocketPath_(GetJobProxyUnixDomainSocketPath())
    , NumaNodeAffinity_(std::move(context.NumaNodeAffinity))
    , Logger(SlotLogger().WithTag("SlotIndex", SlotIndex_))
{
    Location_->IncreaseSessionCount();
    if (context.DiskRequest.disk_space() > 0) {
        Location_->AcquireDiskSpace(SlotIndex_, context.DiskRequest.disk_space());
    }
}

TUserSlotBase::~TUserSlotBase()
{
    YT_TLOG_FATAL_IF(IsEnabled_.load(), "UserSlot was not manually disabled before destruction");
}

void TUserSlotBase::ResetState()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    bool wasEnabled = IsEnabled_.exchange(false);

    YT_TLOG_FATAL_UNLESS(wasEnabled, "Attempt to disable already disabled UserSlot");
    Location_->ReleaseDiskSpace(SlotIndex_);
    Location_->DecreaseSessionCount();
    SlotGuard_.reset();
}

TFuture<void> TUserSlotBase::CleanProcesses()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    if (CleanProcessesFuture_ &&
        (!CleanProcessesFuture_.IsSet() || CleanProcessesFuture_.GetOrCrash().IsOK()))
    {
        return CleanProcessesFuture_;
    }

    PreparationCanceled_ = true;

    CleanProcessesFuture_ = JobProxyPreparationFuture_.Apply(BIND(
        &IJobEnvironment::CleanProcesses,
        JobEnvironment_,
        SlotIndex_,
        SlotGuard_->GetSlotType())
        .AsyncVia(Bootstrap_->GetJobInvoker()))
        .Apply(BIND([Logger = Logger] (const TError& error) {
            YT_TLOG_ERROR_IF(!error.IsOK(), "Failed to clean processes")
                .With(error);
            return error;
        }))
        .ToUncancelable();

    return CleanProcessesFuture_;
}

void TUserSlotBase::CleanJobWorkspaceResources(const TJobFSSecretary& /*fsSecretary*/)
{ }

void TUserSlotBase::CleanAllocationWorkspace()
{ }

void TUserSlotBase::CleanSandbox()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    WaitFor(Location_->CleanSandboxes(
        SlotIndex_))
        .ThrowOnError();
}

void TUserSlotBase::CancelPreparation()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    PreparationCanceled_ = true;
}

void TUserSlotBase::Prepare()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    YT_VERIFY(!CleanProcessesFuture_ || CleanProcessesFuture_.IsSet());

    JobEnvironment_->PrepareSlotForJob(SlotIndex_);

    PreparationCanceled_ = false;
    CleanProcessesFuture_.Reset();
}

TFuture<void> TUserSlotBase::BuildSlotRootDirectory()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "BuildSlotRootDirectory",
        /*uncancelable*/ false,
        [&] {
            return Location_->BuildSlotRootDirectory(SlotIndex_);
        });
}

TFuture<void> TUserSlotBase::RunJobProxy(
    NJobProxy::TJobProxyInternalConfigPtr config,
    TJobId jobId,
    TOperationId operationId)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "RunJobProxy",
        // Job proxy preparation is uncancelable, otherwise we might try to kill
        // a never-started job proxy process.
        /*uncancelable*/ true,
        [&] {
            auto preparationPromise = NewPromise<void>();
            JobProxyPreparationFuture_ = preparationPromise.ToFuture();
            auto finally = Finally([&] {
                preparationPromise.Set();
            });

            if (auto delay = config->JobTestingOptions->DelayBeforeRunJobProxy) {
                YT_TLOG_DEBUG("Testing delay before run job proxy");
                TDelayedExecutor::WaitForDuration(*delay);
            }

            // Enrich the configuration with the environment-specific parameters.
            JobEnvironment_->EnrichJobEnvironmentConfig(SlotIndex_, config);

            YT_TLOG_DEBUG("Start making job proxy config")
                .With("JobId", jobId);

            {
                auto error = WaitFor(Location_->MakeConfig(SlotIndex_, ConvertToNode(config)));
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to create job proxy config");
            }

            YT_TLOG_DEBUG("Finish making job proxy config")
                .With("JobId", jobId);

            if (PreparationCanceled_) {
                return MakeFuture<void>(TError("Job preparation canceled")
                    .With("slot_index", SlotIndex_));
            }

            auto result = JobEnvironment_->RunJobProxy(
                config,
                SlotGuard_->GetSlotType(),
                SlotIndex_,
                Location_->GetSlotPath(SlotIndex_),
                jobId,
                operationId,
                NumaNodeAffinity_);

            if (auto delay = config->JobTestingOptions->DelayAfterRunJobProxy) {
                YT_TLOG_DEBUG("Testing delay after run job proxy");
                TDelayedExecutor::WaitForDuration(*delay);
            }

            return result;
        });
}

TFuture<void> TUserSlotBase::MakeLink(
    TJobId jobId,
    const std::string& artifactName,
    ESandboxKind sandboxKind,
    const std::string& targetPath,
    const std::string& linkName,
    bool executable)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "MakeLink",
        /*uncancelable*/ false,
        [&] {
            return Location_->MakeSandboxLink(
                jobId,
                SlotIndex_,
                artifactName,
                sandboxKind,
                targetPath,
                linkName,
                executable);
        });
}

TFuture<void> TUserSlotBase::MakeFileForSandboxBind(
    TJobId jobId,
    const std::string& artifactName,
    ESandboxKind sandboxKind,
    const std::string& targetPath,
    const std::string& bindPath,
    bool executable)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "MakeBind",
        /*uncancelable*/ false,
        [&] {
            return Location_->MakeFileForSandboxBind(
                jobId,
                SlotIndex_,
                artifactName,
                sandboxKind,
                targetPath,
                bindPath,
                executable);
        });
}

TFuture<void> TUserSlotBase::MakeCopy(
    TJobId jobId,
    const std::string& artifactName,
    ESandboxKind sandboxKind,
    const std::string& sourcePath,
    const TFile& destinationFile,
    const TCacheLocationPtr& sourceLocation)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "MakeCopy",
        /*uncancelable*/ false,
        [&] {
            return Location_->MakeSandboxCopy(
                jobId,
                SlotIndex_,
                artifactName,
                sandboxKind,
                sourcePath,
                destinationFile,
                sourceLocation);
        });
}

TFuture<void> TUserSlotBase::MakeFile(
    TJobId jobId,
    const std::string& artifactName,
    ESandboxKind sandboxKind,
    const std::function<void(IOutputStream*)>& producer,
    const TFile& destinationFile)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "MakeFile",
        /*uncancelable*/ false,
        [&] {
            return Location_->MakeSandboxFile(
                jobId,
                SlotIndex_,
                artifactName,
                sandboxKind,
                producer,
                destinationFile);
        });
}

int TUserSlotBase::GetSlotIndex() const
{
    VerifyEnabled();

    return SlotIndex_;
}

TDiskStatistics TUserSlotBase::GetDiskStatistics() const
{
    VerifyEnabled();

    return Location_->GetDiskStatistics(SlotIndex_);
}

std::string TUserSlotBase::GetSlotPath() const
{
    VerifyEnabled();

    return Location_->GetSlotPath(SlotIndex_);
}

std::string TUserSlotBase::GetMediumName() const
{
    VerifyEnabled();

    return Location_->GetMediumName();
}

TBusServerConfigPtr TUserSlotBase::GetBusServerConfig() const
{
    VerifyEnabled();

    return TBusServerConfig::CreateUds(JobProxyUnixDomainSocketPath_);
}

TBusClientConfigPtr TUserSlotBase::GetBusClientConfig() const
{
    VerifyEnabled();

    return TBusClientConfig::CreateUds(JobProxyUnixDomainSocketPath_);
}

TBusServerConfigPtr TUserSlotBase::GetPrivateBusServerConfig() const
{
    VerifyEnabled();

    return TBusServerConfig::CreateUds(GetJobProxyPrivateUnixDomainSocketPath());
}

TBusClientConfigPtr TUserSlotBase::GetPrivateBusClientConfig() const
{
    VerifyEnabled();

    return TBusClientConfig::CreateUds(GetJobProxyPrivateUnixDomainSocketPath());
}

NRpc::NGrpc::TServerConfigPtr TUserSlotBase::GetGrpcServerConfig() const
{
    VerifyEnabled();

    auto shortPath = NFS::GetRelativePath(
        Location_->GetSlotPath(SlotIndex_),
        GetJobProxyGrpcUnixDomainSocketPath());

    auto addressConfig = New<NRpc::NGrpc::TServerAddressConfig>();
    addressConfig->Address = "unix:" + shortPath;

    auto config = New<NRpc::NGrpc::TServerConfig>();
    config->Addresses.push_back(std::move(addressConfig));

    return config;
}

TFuture<void> TUserSlotBase::PrepareSandboxDirectories(
    const TUserSandboxOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "PrepareSandboxDirectories",
        // Includes quota setting and tmpfs creation.
        /*uncancelable*/ true,
        [&] {
            return Location_->PrepareSandboxDirectories(
                SlotIndex_,
                options);
        });
}

TFuture<std::vector<TShellCommandResult>> TUserSlotBase::RunPreparationCommands(
    TJobId jobId,
    const std::vector<TShellCommandConfigPtr>& commands,
    const NContainers::TRootFS& rootFS,
    const std::string& user,
    const std::optional<std::vector<TDevice>>& devices,
    const std::optional<std::string>& hostName,
    const std::vector<TIP6Address>& ipAddresses,
    std::string tag,
    bool throwOnFailedCommand)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return RunPreparationAction(
        /*actionName*/ "RunPreparationCommands",
        /*uncancelable*/ true,
        [&] {
            return JobEnvironment_->RunCommands(
                SlotIndex_,
                SlotGuard_->GetSlotType(),
                jobId,
                commands,
                rootFS,
                user,
                devices,
                hostName,
                ipAddresses,
                std::move(tag),
                throwOnFailedCommand);
        });
}

void TUserSlotBase::OnArtifactPreparationFailed(
    TJobId jobId,
    const std::string& artifactName,
    ESandboxKind sandboxKind,
    const std::string& artifactPath,
    const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    Location_->OnArtifactPreparationFailed(
        jobId,
        SlotIndex_,
        artifactName,
        sandboxKind,
        artifactPath,
        error);
}

TJobWorkspaceBuilderPtr TUserSlotBase::CreateJobWorkspaceBuilder(
    IInvokerPtr invoker,
    TJobWorkspaceBuildingContext context)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    return JobEnvironment_->CreateJobWorkspaceBuilder(
        invoker,
        std::move(context),
        Location_->GetJobDirectoryManager());
}

int TUserSlotBase::GetUserId() const
{
    VerifyEnabled();

    return JobEnvironment_->GetUserId(SlotIndex_);
}

void TUserSlotBase::SetAllocationId(TAllocationId allocationId)
{
    VerifyEnabled();

    Logger.AddTag("AllocationId", allocationId);
}

std::string TUserSlotBase::GetJobProxyUnixDomainSocketPath() const
{
    VerifyEnabled();

    return NFS::CombinePaths({
        Location_->GetSlotPath(SlotIndex_),
        "pipes",
        Format("%v-job-proxy-%v", NodeTag_, SlotIndex_)});
}

std::string TUserSlotBase::GetJobProxyHttpUnixDomainSocketPath() const
{
    VerifyEnabled();

    return NFS::CombinePaths({
        Location_->GetSlotPath(SlotIndex_),
        "pipes",
        Format("%v-job-proxy-http-%v", NodeTag_, SlotIndex_)});
}

std::string TUserSlotBase::GetSandboxPath(ESandboxKind sandboxKind, const IVolumePtr& rootVolume) const
{
    VerifyEnabled();

    switch (sandboxKind) {
        case ESandboxKind::User:
            if (rootVolume) {
                YT_VERIFY(!rootVolume->GetPath().empty());

                // Use user sandbox within root volume.
                return NFS::CombinePaths(
                    rootVolume->GetPath(),
                    Format("slot/%v", GetSandboxRelPath(sandboxKind)));
            }
            [[fallthrough]];
        default:
            return Location_->GetSandboxPath(
                SlotIndex_,
                sandboxKind);
    }
}

TFuture<void> TUserSlotBase::ValidateRootFS(const IVolumePtr& /*rootVolume*/) const
{
    VerifyEnabled();
    return OKFuture;
}

void TUserSlotBase::ValidateEnabled() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    if (!IsEnabled_.load()) {
        THROW_ERROR_EXCEPTION("User slot is disabled")
            .With("slot_index", SlotIndex_);
    }

    Location_->ValidateEnabled();
}

TString TUserSlotBase::GetRootPath(const IVolumePtr& rootVolume) const
{
    VerifyEnabled();

    if (rootVolume) {
        YT_VERIFY(!rootVolume->GetPath().empty());
        return TString(rootVolume->GetPath());
    } else {
        return Location_->GetSlotPath(SlotIndex_);
    }
}

void TUserSlotBase::VerifyEnabled() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_TLOG_FATAL_UNLESS(IsEnabled_.load(), "Accessing disabled UserSlot");
}

std::string TUserSlotBase::GetJobProxyGrpcUnixDomainSocketPath() const
{
    VerifyEnabled();

    return NFS::CombinePaths({
        Location_->GetSlotPath(SlotIndex_),
        "pipes",
        Format("%v-job-proxy-grpc-%v", NodeTag_, SlotIndex_)});
}

std::string TUserSlotBase::GetJobProxyPrivateUnixDomainSocketPath() const
{
    VerifyEnabled();

    return NFS::CombinePaths(
        Location_->GetSandboxPath(SlotIndex_, ESandboxKind::Private),
        Format("%v-private-%v", NodeTag_, SlotIndex_));
}

TFuture<std::vector<TVolumeResultPtr>> TUserSlotBase::DoPrepareNonRootVolumes(
    const IVolumePtr& rootVolume,
    const std::vector<TBaseVolumeParamsPtr>& volumeParams,
    const std::vector<TVolumeMountPtr>& volumeMounts,
    const TCallback<TFuture<std::vector<TVolumeResultPtr>>(const std::string&)>& prepareAction)
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    VerifyEnabled();

    YT_TLOG_DEBUG("Preparing non-root volumes")
        .With("Volumes", MakeFormattableView(
            volumeParams,
            [] (TNonNullPtr<TStringBuilderBase> builder, const TBaseVolumeParamsPtr& volume) {
                builder->AppendFormat("{VolumeId: %v}", volume->VolumeId);
            }));

    if (!VolumeManager_) {
        static constexpr auto Message = "Failed to prepare non-root volumes since volume manager is not initialized"_sb;
        YT_TLOG_WARNING(Message);
        return MakeFuture<std::vector<TVolumeResultPtr>>(TError(Message));
    }

    auto userSandboxPath = GetSandboxPath(ESandboxKind::User, rootVolume);
    return RunPreparationAction(
        /*actionName*/ "PrepareNonRootVolumes",
        /*uncancelable*/ false,
        [&] {
            if (!Bootstrap_->GetConfig()->ExecNode->SlotManager->EnableNonRootVolumes) {
                for (const auto& volume : volumeParams) {
                    if (!volume->LayerArtifactKeys.GetAll().empty()) {
                        THROW_ERROR_EXCEPTION(
                            "Cannot create fake non-root volume %v since it contains a layer",
                            volume->VolumeId);
                    }
                }

                return Location_->CreateFakeNonRootVolumes(rootVolume, SlotIndex_, volumeMounts)
                    .AsUnique()
                    .Apply(
                        BIND([] (TError&& error) {
                            if (!error.IsOK()) {
                                THROW_ERROR_EXCEPTION(
                                    "Failed to prepare fake non-root volumes: %v",
                                    error);
                            }
                            return MakeFuture(std::vector<TVolumeResultPtr>{});
                        }));
            }

            return prepareAction(userSandboxPath)
                .AsUnique()
                .Apply(
                    BIND(
                        [
                            rootVolume,
                            volumeMounts,
                            this,
                            this_ = MakeStrong(this)
                        ] (TErrorOr<std::vector<TVolumeResultPtr>>&& volumeResultsOrError) {
                            if (!volumeResultsOrError.IsOK()) {
                                THROW_ERROR_EXCEPTION("Failed to prepare non-root volumes: %v",
                                    volumeResultsOrError);
                            }

                            auto& volumeResults = volumeResultsOrError.Value();

                            // Inform slot location about non-root volumes to be used.
                            Location_->TakeIntoAccountNonRootVolumes(
                                SlotIndex_,
                                rootVolume,
                                volumeResults,
                                volumeMounts);
                            return std::move(volumeResults);
                })
                .AsyncVia(Bootstrap_->GetJobInvoker()));
        });
}

////////////////////////////////////////////////////////////////////////////////

IUserSlotPtr CreateSlot(
    TNonNullPtr<TSlotManager> slotManager,
    TSlotLocationPtr location,
    IJobEnvironmentPtr environment,
    IVolumeManagerPtr volumeManager,
    TNonNullPtr<NExecNode::IBootstrap> bootstrap,
    const std::string& nodeTag,
    ESlotType slotType,
    NClusterNode::TCpu requestedCpu,
    NScheduler::NProto::TDeprecatedDiskRequest diskRequest,
    const std::optional<TNumaNodeInfo>& numaNodeAffinity)
{
    TUserSlotContext context{
        .SlotManager = slotManager,
        .Location = std::move(location),
        .Environment = environment,
        .VolumeManager = std::move(volumeManager),
        .Bootstrap = bootstrap,
        .NodeTag = nodeTag,
        .SlotType = slotType,
        .RequestedCpu = requestedCpu,
        .DiskRequest = std::move(diskRequest),
        .NumaNodeAffinity = numaNodeAffinity,
    };
    return environment->CreateUserSlot(std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
