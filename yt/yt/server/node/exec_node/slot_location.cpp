#include "slot_location.h"

#include "bootstrap.h"
#include "slot_manager.h"
#include "private.h"
#include "job_directory_manager.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/location.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/fs.h>

#include <util/folder/path.h>

#include <util/stream/length.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NTools;
using namespace NYson;
using namespace NYTree;
using namespace NIO;
using namespace NDataNode;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TSlotLocation::TSlotLocation(
    TSlotLocationConfigPtr config,
    IBootstrap* bootstrap,
    const TString& id,
    IJobDirectoryManagerPtr jobDirectoryManager,
    bool enableTmpfs,
    int slotCount,
    std::function<int(int)> slotIndexToUserId)
    : TDiskLocation(config, id, ExecNodeLogger)
    , Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , JobDirectoryManager_(std::move(jobDirectoryManager))
    , EnableTmpfs_(enableTmpfs)
    , SlotCount_(slotCount)
    , SlotIndexToUserId_(slotIndexToUserId)
    , HeavyLocationQueue_(New<TActionQueue>(Format("HeavyIO:%v", id)))
    , LightLocationQueue_(New<TActionQueue>(Format("LightIO:%v", id)))
    , HeavyInvoker_(HeavyLocationQueue_->GetInvoker())
    , LightInvoker_(LightLocationQueue_->GetInvoker())
    , SerializedHeavyInvoker_(CreateSerializedInvoker(HeavyInvoker_))
    , HealthChecker_(New<TDiskHealthChecker>(
        bootstrap->GetConfig()->DataNode->DiskHealthChecker,
        Config_->Path,
        HeavyInvoker_,
        Logger))
    , DiskResourcesUpdateExecutor_(New<TPeriodicExecutor>(
        HeavyInvoker_,
        BIND(&TSlotLocation::UpdateDiskResources, MakeWeak(this)),
        Bootstrap_->GetConfig()->ExecNode->SlotManager->DiskResourcesUpdatePeriod))
    , SlotLocationStatisticsUpdateExecutor_(New<TPeriodicExecutor>(
        HeavyLocationQueue_->GetInvoker(),
        BIND(&TSlotLocation::UpdateSlotLocationStatistics, MakeWeak(this)),
        Bootstrap_->GetConfig()->ExecNode->SlotManager->SlotLocationStatisticsUpdatePeriod))
    , LocationPath_(NFS::GetRealPath(Config_->Path))
{ }

TFuture<void> TSlotLocation::Initialize()
{
    ChangeState(ELocationState::Enabled);

    return BIND([=, this, this_ = MakeStrong(this)] {
        try {
            DoInitialize();
        } catch (const std::exception& ex) {
            auto error = TError("Failed to initialize slot location %v", Config_->Path)
                << ex;
            Disable(error);
            return;
        }
        HealthChecker_->SubscribeFailed(BIND(&TSlotLocation::Disable, MakeWeak(this))
            .Via(HeavyInvoker_));
        HealthChecker_->Start();

        Bootstrap_->SubscribePopulateAlerts(BIND(&TSlotLocation::PopulateAlerts, MakeWeak(this)));

    })
    .AsyncVia(SerializedHeavyInvoker_)
    .Run();
}

void TSlotLocation::DoInitialize()
{
    VERIFY_INVOKER_AFFINITY(SerializedHeavyInvoker_);

    YT_LOG_INFO("Location initialization started");

    NFS::MakeDirRecursive(Config_->Path, 0755);

    WaitFor(HealthChecker_->RunCheck())
        .ThrowOnError();

    ValidateMinimumSpace();

    for (int slotIndex = 0; slotIndex < SlotCount_; ++slotIndex) {
        BuildSlotRootDirectory(slotIndex);
    }

    DiskResourcesUpdateExecutor_->Start();
    SlotLocationStatisticsUpdateExecutor_->Start();

    YT_LOG_INFO("Location initialization complete");
}

void TSlotLocation::DoRepair(bool force)
{
    if (GetState() == ELocationState::Enabled && !force) {
        YT_LOG_DEBUG("Skipping location repair as it is already enabled (Location: %v)", Id_);
        return;
    }

    try {
        {
            auto guard = WriterGuard(SlotsLock_);
            SandboxOptionsPerSlot_.clear();
            TmpfsPaths_.clear();
            SlotsWithQuota_.clear();
        }

        {
            auto guard = WriterGuard(DiskResourcesLock_);
            ReservedDiskSpacePerSlot_.clear();
            DiskResources_.set_usage(0);
        }

        WaitFor(JobDirectoryManager_->CleanDirectories(Config_->Path))
            .ThrowOnError();

        Error_.Store(TError{});
        Alert_.Store(TError{});

        DoInitialize();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to repair slot location %v", Config_->Path)
            << ex;
        THROW_ERROR error;
    }

    ChangeState(ELocationState::Enabled);

    YT_LOG_DEBUG("Location repaired (Location: %v)", Id_);
}

std::vector<TString> TSlotLocation::DoPrepareSandboxDirectories(int slotIndex, TUserSandboxOptions options, bool sandboxInsideTmpfs)
{
    ValidateEnabled();

    YT_LOG_DEBUG("Preparing sandbox directiories (SlotIndex: %v, SandboxInsideTmpfs: %v)",
        slotIndex,
        sandboxInsideTmpfs);

    auto userId = SlotIndexToUserId_(slotIndex);
    auto sandboxPath = GetSandboxPath(slotIndex, ESandboxKind::User);

    bool shouldApplyQuota = ((options.InodeLimit || options.DiskSpaceLimit) && !sandboxInsideTmpfs);
    if (shouldApplyQuota) {
        try {
            auto properties = TJobDirectoryProperties {options.DiskSpaceLimit, options.InodeLimit, userId};
            WaitFor(JobDirectoryManager_->ApplyQuota(sandboxPath, properties))
                .ThrowOnError();
            {
                auto guard = WriterGuard(SlotsLock_);
                SlotsWithQuota_.insert(slotIndex);
            }
        } catch (const std::exception& ex) {
            auto error = TError(EErrorCode::QuotaSettingFailed, "Failed to set FS quota for a job sandbox")
                << TErrorAttribute("sandbox_path", sandboxPath)
                << ex;
            Disable(error);
            THROW_ERROR error;
        }
    }

    // This tmp sandbox is a temporary workaround for nirvana. We apply the same quota as we do for usual sandbox.
    if (options.DiskSpaceLimit || options.InodeLimit) {
        auto tmpPath = GetSandboxPath(slotIndex, ESandboxKind::Tmp);
        try {
            auto properties = TJobDirectoryProperties{options.DiskSpaceLimit, options.InodeLimit, userId};
            WaitFor(JobDirectoryManager_->ApplyQuota(tmpPath, properties))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            auto error = TError(EErrorCode::QuotaSettingFailed, "Failed to set FS quota for a job tmp directory")
                << TErrorAttribute("tmp_path", tmpPath)
                << ex;
            Disable(error);
            THROW_ERROR error;
        }
    }

    {
        auto guard = WriterGuard(SlotsLock_);
        EmplaceOrCrash(SandboxOptionsPerSlot_, slotIndex, options);
    }

    std::vector<TString> result;

    for (const auto& tmpfsVolume : options.TmpfsVolumes) {
        // TODO(gritukan): GetRealPath here can be replaced with some light analogue that does not access filesystem.
        auto tmpfsPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, tmpfsVolume.Path));
        try {
            if (tmpfsPath != sandboxPath) {
                // If we mount directory inside sandbox, it should not exist.
                ValidateNotExists(tmpfsPath);
            }
            NFS::MakeDirRecursive(tmpfsPath);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create directory %v for tmpfs in sandbox %v",
                tmpfsPath,
                sandboxPath)
                << ex;
        }

        if (!EnableTmpfs_) {
            continue;
        }

        try {
            auto properties = TJobDirectoryProperties{tmpfsVolume.Size, std::nullopt, userId};
            WaitFor(JobDirectoryManager_->CreateTmpfsDirectory(tmpfsPath, properties))
                .ThrowOnError();

            {
                auto guard = WriterGuard(SlotsLock_);
                YT_VERIFY(TmpfsPaths_.insert(tmpfsPath).second);
            }

            result.push_back(tmpfsPath);
        } catch (const std::exception& ex) {
            // Job will be aborted.
            auto error = TError(EErrorCode::SlotLocationDisabled, "Failed to mount tmpfs %v into sandbox %v", tmpfsPath, sandboxPath)
                << ex;
            Disable(error);
            THROW_ERROR error;
        }
    }

    for (int i = 0; i < std::ssize(result); ++i) {
        for (int j = 0; j < std::ssize(result); ++j) {
            if (i == j) {
                continue;
            }
            auto lhsFsPath = TFsPath(result[i]);
            auto rhsFsPath = TFsPath(result[j]);
            if (lhsFsPath.IsSubpathOf(rhsFsPath)) {
                THROW_ERROR_EXCEPTION("Path of tmpfs volume %v is prefix of other tmpfs volume %v",
                    result[i],
                    result[j]);
            }
        }
    }

    YT_LOG_DEBUG("Sandbox directories prepared (SlotIndex: %v)",
        slotIndex);

    return result;
}

TFuture<std::vector<TString>> TSlotLocation::PrepareSandboxDirectories(int slotIndex, TUserSandboxOptions options)
{
    auto sandboxPath = GetSandboxPath(slotIndex, ESandboxKind::User);

    bool sandboxInsideTmpfs = WaitFor(
        BIND([=] {
            for (const auto& tmpfsVolume : options.TmpfsVolumes) {
                // TODO(gritukan): Implement a function that joins absolute path with a relative path and returns
                // real path without filesystem access.
                auto tmpfsPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, tmpfsVolume.Path));
                if (tmpfsPath == sandboxPath) {
                    return true;
                }
            }

            return false;
        })
        .AsyncVia(LightInvoker_)
        .Run())
        .ValueOrThrow();

    const auto& invoker = sandboxInsideTmpfs
        ? LightInvoker_
        : HeavyInvoker_;

    return BIND(&TSlotLocation::DoPrepareSandboxDirectories, MakeStrong(this), slotIndex, options, sandboxInsideTmpfs)
        .AsyncVia(invoker)
        .Run();
}

TFuture<void> TSlotLocation::DoMakeSandboxFile(
    TJobId jobId,
    int slotIndex,
    const TString& artifactName,
    ESandboxKind sandboxKind,
    const TCallback<void()>& callback,
    const std::optional<TString>& destinationPath,
    bool canUseLightInvoker)
{
    if (destinationPath) {
        canUseLightInvoker &= IsInsideTmpfs(*destinationPath);
    }

    const auto& invoker = canUseLightInvoker
        ? LightInvoker_
        : HeavyInvoker_;

    YT_LOG_DEBUG(
        "Started making sandbox file "
        "(JobId: %v, ArtifactName: %v, SandboxKind: %v, UseLightInvoker: %v)",
        jobId,
        artifactName,
        sandboxKind,
        canUseLightInvoker);

    return BIND([=, this, this_ = MakeStrong(this)] {
        ValidateEnabled();

        auto onError = [&] (const TError& error) {
            OnArtifactPreparationFailed(
                jobId,
                slotIndex,
                artifactName,
                sandboxKind,
                destinationPath,
                error);
        };

        try {
            callback();
        } catch (const TSystemError& ex) {
            // For util functions.
            onError(TError::FromSystem(ex));
        } catch (const std::exception& ex) {
            onError(TError(ex));
        }
    })
    .AsyncVia(invoker)
    .Run();
}

struct TSlotLocationIOTags
{
    int SlotIndex;
};

static THashMap<TString, TString> BuildSandboxCopyTags(
    const char* direction,
    const NDataNode::TChunkLocationPtr& location,
    const std::optional<TSlotLocationIOTags>& slotLocationTags)
{
    THashMap<TString, TString> result{
        {FormatIOTag(EAggregateIOTag::Direction), direction},
        {FormatIOTag(EAggregateIOTag::User), GetCurrentAuthenticationIdentity().User},
    };
    if (location) {
        result[FormatIOTag(ERawIOTag::LocationId)] = location->GetId();
        result[FormatIOTag(EAggregateIOTag::LocationType)] = FormatEnum(location->GetType());
        result[FormatIOTag(EAggregateIOTag::Medium)] = location->GetMediumName();
        result[FormatIOTag(EAggregateIOTag::DiskFamily)] = location->GetDiskFamily();
    }
    if (slotLocationTags) {
        result[FormatIOTag(EAggregateIOTag::LocationType)] = "slot";
        result[FormatIOTag(ERawIOTag::SlotIndex)] = ToString(slotLocationTags->SlotIndex);
    }
    return result;
}

TFuture<void> TSlotLocation::MakeSandboxCopy(
    TJobId jobId,
    int slotIndex,
    const TString& artifactName,
    ESandboxKind sandboxKind,
    const TString& sourcePath,
    const TFile& destinationFile,
    const NDataNode::TChunkLocationPtr& sourceLocation)
{
    return DoMakeSandboxFile(
        jobId,
        slotIndex,
        artifactName,
        sandboxKind,
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG(
                "Started copying file to sandbox "
                "(JobId: %v, ArtifactName: %v, SandboxKind: %v, SourcePath: %v, DestinationPath: %v)",
                jobId,
                artifactName,
                sandboxKind,
                sourcePath,
                destinationFile.GetName());

            TFile sourceFile(sourcePath, OpenExisting | RdOnly | Seq | CloseOnExec);
            NFS::ChunkedCopy(
                sourceFile,
                destinationFile,
                Bootstrap_->GetConfig()->ExecNode->SlotManager->FileCopyChunkSize);

            if (Bootstrap_->GetIOTracker()->IsEnabled()) {
                TString fullArtifactPath = NFS::CombinePaths(GetSandboxPath(slotIndex, sandboxKind), artifactName);

                Bootstrap_->GetIOTracker()->Enqueue(
                    TIOCounters{
                        .Bytes = sourceFile.GetLength(),
                        .IORequests = 1,
                    },
                    /*tags*/ BuildSandboxCopyTags(
                        "read",
                        sourceLocation,
                        /*slotLocationTags*/ std::nullopt));

                if (!IsInsideTmpfs(fullArtifactPath)) {
                    Bootstrap_->GetIOTracker()->Enqueue(
                        TIOCounters{
                            .Bytes = sourceFile.GetLength(),
                            .IORequests = 1,
                        },
                        /*tags*/ BuildSandboxCopyTags(
                            "write",
                            /*location*/ nullptr,
                            /*slotLocationTags*/ TSlotLocationIOTags{.SlotIndex = slotIndex}));
                }
            }

            YT_LOG_DEBUG(
                "Finished copying file to sandbox "
                "(JobId: %v, ArtifactName: %v, SandboxKind: %v)",
                jobId,
                artifactName,
                sandboxKind);
        }),
        /*destinationPath*/ std::nullopt,
        /*canUseLightInvoker*/ IsInsideTmpfs(sourcePath));
}

TFuture<void> TSlotLocation::MakeSandboxLink(
    TJobId jobId,
    int slotIndex,
    const TString& artifactName,
    ESandboxKind sandboxKind,
    const TString& targetPath,
    const TString& linkPath,
    bool executable)
{
    return DoMakeSandboxFile(
        jobId,
        slotIndex,
        artifactName,
        sandboxKind,
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG(
                "Started making sandbox symlink "
                "(JobId: %v, ArtifactName: %v, SandboxKind: %v, TargetPath: %v, LinkPath: %v)",
                jobId,
                artifactName,
                sandboxKind,
                targetPath,
                linkPath);

            auto sandboxPath = GetSandboxPath(slotIndex, sandboxKind);
            try {
                // These validations do not disable slot.
                ValidateNotExists(linkPath);
                ForceSubdirectories(linkPath, sandboxPath);
            } catch (const std::exception& ex) {
                // Job will be failed.
                THROW_ERROR_EXCEPTION(
                    "Failed to build file %Qv in sandbox %Qv",
                    artifactName,
                    sandboxKind)
                    << ex;
            }

            // NB: Set permissions for the link _source_ and prevent writes to it.
            NFS::SetPermissions(targetPath, 0644 + (executable ? 0111 : 0));

            NFS::MakeSymbolicLink(targetPath, linkPath);

            EnsureNotInUse(targetPath);

            YT_LOG_DEBUG("Finished making sandbox symlink "
                "(JobId: %v, ArtifactName: %v, SandboxKind: %v)",
                jobId,
                artifactName,
                sandboxKind);
        }),
        /*destinationPath*/ targetPath,
        /*canUseLightInvoker*/ true);
}

TFuture<void> TSlotLocation::MakeSandboxFile(
    TJobId jobId,
    int slotIndex,
    const TString& artifactName,
    ESandboxKind sandboxKind,
    const std::function<void(IOutputStream*)>& producer,
    const TFile& destinationFile)
{
    return DoMakeSandboxFile(
        jobId,
        slotIndex,
        artifactName,
        sandboxKind,
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_LOG_DEBUG(
                "Started building sandbox file "
                "(JobId: %v, ArtifactName: %v, SandboxKind: %v, DestinationPath: %v)",
                jobId,
                artifactName,
                sandboxKind,
                destinationFile.GetName());

            TFileOutput stream(destinationFile);
            TCountingOutput countingStream(&stream);
            producer(&countingStream);

            if (Bootstrap_->GetIOTracker()->IsEnabled()) {
                TString fullArtifactPath = NFS::CombinePaths(GetSandboxPath(slotIndex, sandboxKind), artifactName);

                if (!IsInsideTmpfs(fullArtifactPath)) {
                    Bootstrap_->GetIOTracker()->Enqueue(
                        TIOCounters{
                            .Bytes = static_cast<i64>(countingStream.Counter()),
                            .IORequests = 1,
                        },
                        /*tags*/ {
                            {FormatIOTag(EAggregateIOTag::Direction), "write"},
                            {FormatIOTag(EAggregateIOTag::User), GetCurrentAuthenticationIdentity().User},
                            {FormatIOTag(EAggregateIOTag::LocationType), "slot"},
                            {FormatIOTag(ERawIOTag::SlotIndex), ToString(slotIndex)},
                        });
                }
            }

            YT_LOG_DEBUG(
                "Finished building sandbox file "
                "(JobId: %v, ArtifactName: %v, SandboxKind: %v)",
                jobId,
                artifactName,
                sandboxKind);
        }),
        /*destinationPath*/ std::nullopt,
        /*canUseLightInvoker*/ true);
}

TFuture<void> TSlotLocation::MakeConfig(int slotIndex, INodePtr config)
{
    return BIND([=, this, this_ = MakeStrong(this)] {
        YT_LOG_DEBUG("Making job proxy config (SlotIndex: %v)",
            slotIndex);

        ValidateEnabled();
        auto proxyConfigPath = GetConfigPath(slotIndex);

        try {
            TFile file(proxyConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
            TUnbufferedFileOutput output(file);
            TYsonWriter writer(&output, EYsonFormat::Pretty);
            Serialize(config, &writer);
            writer.Flush();
        } catch (const std::exception& ex) {
            // Job will be aborted.
            auto error = TError(EErrorCode::SlotLocationDisabled, "Failed to write job proxy config into %v",
                proxyConfigPath)
                << ex;
            Disable(error);
            THROW_ERROR error;
        }

        YT_LOG_DEBUG("Job proxy config written (SlotIndex: %v)",
            slotIndex);
    })
    // NB(gritukan): Job proxy config is written to the disk, but it should be fast
    // under reasonable circumstances, so we use light invoker here.
    .AsyncVia(LightInvoker_)
    .Run();
}

TFuture<void> TSlotLocation::CleanSandboxes(int slotIndex)
{
    return BIND([=, this, this_ = MakeStrong(this)] {
        YT_LOG_DEBUG("Sandboxes cleaning started (SlotIndex: %v)",
            slotIndex);

        ValidateEnabled();

        {
            auto guard = WriterGuard(SlotsLock_);

            // There may be no slotIndex in this map
            // (e.g. during SlotMananager::Initialize)
            SandboxOptionsPerSlot_.erase(slotIndex);
        }

        try {
            for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
                const auto& sandboxPath = GetSandboxPath(slotIndex, sandboxKind);
                if (!NFS::Exists(sandboxPath)) {
                    continue;
                }

                YT_LOG_DEBUG("Removing job directories (Path: %v)", sandboxPath);

                WaitFor(JobDirectoryManager_->CleanDirectories(sandboxPath))
                    .ThrowOnError();

                YT_LOG_DEBUG("Cleaning sandbox directory (Path: %v)", sandboxPath);

                if (Bootstrap_->IsSimpleEnvironment()) {
                    NFS::RemoveRecursive(sandboxPath);
                } else {
                    RunTool<TRemoveDirAsRootTool>(sandboxPath);
                }

                {
                    auto guard = WriterGuard(SlotsLock_);

                    auto it = TmpfsPaths_.lower_bound(sandboxPath);
                    while (it != TmpfsPaths_.end() && it->StartsWith(sandboxPath)) {
                        it = TmpfsPaths_.erase(it);
                    }

                    SlotsWithQuota_.erase(slotIndex);
                }
            }

            // Prepare slot for the next job.
            BuildSlotRootDirectory(slotIndex);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to clean sandbox directories")
                << ex;
            Disable(error);
            THROW_ERROR error;
        }

        YT_LOG_DEBUG("Sandboxes cleaning finished (SlotIndex: %v)",
            slotIndex);
    })
    .AsyncVia(HeavyInvoker_)
    .Run();
}

void TSlotLocation::IncreaseSessionCount()
{
    ++SessionCount_;
}

void TSlotLocation::DecreaseSessionCount()
{
    --SessionCount_;
}

void TSlotLocation::ValidateNotExists(const TString& path)
{
    if (NFS::Exists(path)) {
        THROW_ERROR_EXCEPTION("Path %v already exists", path);
    }
}

void TSlotLocation::EnsureNotInUse(const TString& path) const
{
    // Take exclusive lock in blocking fashion to ensure that no
    // forked process is holding an open descriptor to the source file.
    TFile file(path, RdOnly | CloseOnExec);
    file.Flock(LOCK_EX);
}

TString TSlotLocation::GetConfigPath(int slotIndex) const
{
    return NFS::CombinePaths(GetSlotPath(slotIndex), ProxyConfigFileName);
}

TString TSlotLocation::GetSlotPath(int slotIndex) const
{
    return NFS::CombinePaths(LocationPath_, Format("%v", slotIndex));
}

TDiskStatistics TSlotLocation::GetDiskStatistics(int slotIndex) const
{
    auto guard = ReaderGuard(SlotsLock_);
    auto it = DiskStatisticsPerSlot_.find(slotIndex);
    return it == DiskStatisticsPerSlot_.end() ? TDiskStatistics{} : it->second;
}

TString TSlotLocation::GetMediumName() const
{
    return Config_->MediumName;
}

NChunkClient::TMediumDescriptor TSlotLocation::GetMediumDescriptor() const
{
    return MediumDescriptor_.Load();
}

void TSlotLocation::SetMediumDescriptor(const NChunkClient::TMediumDescriptor& descriptor)
{
    MediumDescriptor_.Store(descriptor);
}

TString TSlotLocation::GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    YT_ASSERT(sandboxName);
    return NFS::CombinePaths(GetSlotPath(slotIndex), sandboxName);
}

void TSlotLocation::OnArtifactPreparationFailed(
    TJobId jobId,
    int slotIndex,
    const TString& artifactName,
    ESandboxKind sandboxKind,
    const std::optional<TString>& destinationPath,
    const TError& error)
{
    auto Logger = this->Logger
        .WithTag("JobId: %v, ArtifactName: %v, SandboxKind: %v",
            jobId,
            artifactName,
            sandboxKind);

    bool slotWithQuota = false;
    {
        auto guard = ReaderGuard(SlotsLock_);
        slotWithQuota = SlotsWithQuota_.contains(slotIndex);
    }

    bool destinationInsideTmpfs = destinationPath && IsInsideTmpfs(*destinationPath);

    bool brokenPipe = static_cast<bool>(error.FindMatching(ELinuxErrorCode::PIPE));
    bool noSpace =
        static_cast<bool>(error.FindMatching(ELinuxErrorCode::NOSPC)) ||
        static_cast<bool>(error.FindMatching(ELinuxErrorCode::DQUOT));

    // NB: Broken pipe error usually means that job proxy exited abnormally during artifact preparation.
    // We silently ignore it and wait for the job proxy exit error.
    if (brokenPipe) {
        YT_LOG_INFO(error, "Failed to build file in sandbox: broken pipe");

        THROW_ERROR_EXCEPTION(
            EErrorCode::ArtifactCopyingFailed,
            "Failed to build file %Qv in sandbox %Qv: broken pipe",
            artifactName,
            sandboxKind)
            << error;
    } else if (destinationInsideTmpfs && noSpace) {
        YT_LOG_INFO(error, "Failed to build file in sandbox: tmpfs is too small");

        THROW_ERROR_EXCEPTION(
            EErrorCode::TmpfsOverflow,
            "Failed to build file %Qv in sandbox %Qv: tmpfs is too small",
            artifactName,
            sandboxKind)
            << error;
    } else if (slotWithQuota && noSpace) {
        YT_LOG_INFO(error, "Failed to build file in sandbox: disk space limit is too small");

        THROW_ERROR_EXCEPTION(
            "Failed to build file %Qv in sandbox %Qv: disk space limit is too small",
            artifactName,
            sandboxKind)
            << error;
    } else {
        YT_LOG_INFO(error, "Failed to build file in sandbox:");

        auto wrappedError = TError(
            EErrorCode::ArtifactCopyingFailed,
            "Failed to build file %Qv in sandbox %Qv",
            artifactName,
            sandboxKind)
            << error;

        if (IsSystemError(wrappedError)) {
            Disable(wrappedError);
        }
        // Job will be aborted.
        THROW_ERROR wrappedError;
    }
}

TFuture<void> TSlotLocation::Repair(bool force)
{
    return BIND(&TSlotLocation::DoRepair, MakeStrong(this), force)
        .AsyncVia(SerializedHeavyInvoker_)
        .Run();
}

bool TSlotLocation::IsInsideTmpfs(const TString& path) const
{
    auto guard = ReaderGuard(SlotsLock_);

    auto it = TmpfsPaths_.upper_bound(path);
    if (it != TmpfsPaths_.begin()) {
        --it;
        if (path == *it || path.StartsWith(*it + "/")) {
            return true;
        }
    }

    return false;
}

void TSlotLocation::ForceSubdirectories(const TString& filePath, const TString& sandboxPath) const
{
    auto dirPath = NFS::GetDirectoryName(filePath);
    if (!dirPath.StartsWith(sandboxPath)) {
        THROW_ERROR_EXCEPTION("Path of the file must be inside the sandbox directory")
            << TErrorAttribute("sandbox_path", sandboxPath)
            << TErrorAttribute("file_path", filePath);
    }
    NFS::MakeDirRecursive(dirPath);
}

void TSlotLocation::ValidateEnabled() const
{
    if (!IsEnabled()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::SlotLocationDisabled,
            "Slot location at %v is disabled",
            Config_->Path);
    }
}

void TSlotLocation::Disable(const TError& error)
{
    WaitFor(BIND([=, this, this_ = MakeStrong(this)] {
        // TODO(don-dron): Research and fix unconditional Disabled.
        if (State_.exchange(ELocationState::Disabled) != ELocationState::Enabled) {
            return;
        }

        Error_.Store(error);

        auto alert = TError(
            EErrorCode::SlotLocationDisabled,
            "Slot location at %v is disabled",
            Config_->Path)
            << error;

        YT_LOG_ERROR(alert);
        Alert_.Store(alert);

        DiskResourcesUpdateExecutor_->Stop();
        SlotLocationStatisticsUpdateExecutor_->Stop();

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        const auto& dynamicConfig = dynamicConfigManager->GetConfig()->DataNode;
        if (dynamicConfig->AbortOnLocationDisabled) {
            TProgram::Abort(EProgramExitCode::ProgramError);
        }
    })
    .AsyncVia(SerializedHeavyInvoker_)
    .Run())
    .ThrowOnError();
}

void TSlotLocation::InvokeUpdateDiskResources()
{
    DiskResourcesUpdateExecutor_->ScheduleOutOfBand();
}

void TSlotLocation::UpdateDiskResources()
{
    if (!IsEnabled()) {
        return;
    }

    YT_LOG_DEBUG("Updating disk resources");

    try {
        auto locationStatistics = NFS::GetDiskSpaceStatistics(Config_->Path);
        i64 diskLimit = locationStatistics.TotalSpace;
        if (Config_->DiskQuota) {
            diskLimit = Min(diskLimit, *Config_->DiskQuota);
        }

        i64 diskUsage = 0;
        i64 reservedAvailableSpace = 0;

        THashMap<int, TUserSandboxOptions> sandboxOptionsPerSlot;

        {
            auto guard = ReaderGuard(SlotsLock_);
            sandboxOptionsPerSlot = SandboxOptionsPerSlot_;
        }

        THashMap<int, TDiskStatistics> diskStatisticsPerSlot;

        for (const auto& [slotIndex, sandboxOptions] : sandboxOptionsPerSlot) {
            std::vector<TString> pathsInsideTmpfs;

            auto config = New<TGetDirectorySizesAsRootConfig>();
            config->IgnoreUnavailableFiles = true;
            config->DeduplicateByINodes = true;
            config->CheckDeviceId = true;
            for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
                auto path = GetSandboxPath(slotIndex, sandboxKind);
                if (NFS::Exists(path)) {
                    if (IsInsideTmpfs(path)) {
                        pathsInsideTmpfs.push_back(path);
                    } else {
                        config->Paths.push_back(path);
                    }
                }
            }

            i64 slotDiskUsage = 0;
            if (Bootstrap_->IsSimpleEnvironment()) {
                for (const auto& path : config->Paths) {
                    slotDiskUsage += NFS::GetDirectorySize(path, /*ignoreUnavailableFiles*/ true, /*deduplicateByINodes*/ true);
                }
            } else {
                // We have to calculate user directory sizes as root,
                // because user job could have set restricted permissions for files and
                // directories inside sandbox.
                auto sizes = RunTool<TGetDirectorySizesAsRootTool>(std::move(config));
                slotDiskUsage = std::accumulate(sizes.begin(), sizes.end(), 0ll);
            }

            diskStatisticsPerSlot.insert(std::make_pair(
                slotIndex,
                TDiskStatistics{
                    .Limit = sandboxOptions.DiskSpaceLimit,
                    .Usage = slotDiskUsage,
                }));

            const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
            const auto& dynamicConfig = dynamicConfigManager->GetConfig()->ExecNode->SlotManager;
            YT_LOG_DEBUG("Slot disk usage info (Path: %v, SlotIndex: %v, Usage: %v, Limit: %v, PathsInsideTmpfs: %v)",
                Config_->Path,
                slotIndex,
                slotDiskUsage,
                sandboxOptions.DiskSpaceLimit,
                pathsInsideTmpfs);
            if (sandboxOptions.DiskSpaceLimit) {
                i64 slotDiskLimit = *sandboxOptions.DiskSpaceLimit;
                diskUsage += slotDiskLimit;
                reservedAvailableSpace += slotDiskLimit - slotDiskUsage;
                if (dynamicConfig->CheckDiskSpaceLimit && slotDiskUsage > slotDiskLimit) {
                    auto error = TError("Disk usage overdrafted: %v > %v",
                        slotDiskUsage,
                        slotDiskLimit);

                    YT_LOG_INFO(error, "Slot disk usage overdrafted (Path: %v, SlotIndex: %v)",
                        Config_->Path,
                        slotIndex);
                    sandboxOptions.DiskOverdraftCallback
                        .Run(error);
                }
            } else {
                diskUsage += slotDiskUsage;
            }
        }

        {
            auto guard = WriterGuard(SlotsLock_);
            DiskStatisticsPerSlot_ = diskStatisticsPerSlot;
        }

        {
            auto guard = WriterGuard(DiskResourcesLock_);
            for (auto [slotIndex, diskSpace] : ReservedDiskSpacePerSlot_) {
                if (!sandboxOptionsPerSlot.contains(slotIndex)) {
                    diskUsage += diskSpace;
                }
            }

            auto availableSpace = Max<i64>(0, Min(locationStatistics.AvailableSpace - reservedAvailableSpace, diskLimit - diskUsage));
            diskLimit = Min(diskLimit, diskUsage + availableSpace);
            diskLimit -= Config_->DiskUsageWatermark;

            YT_LOG_DEBUG("Disk info (Path: %v, Usage: %v, Limit: %v, Medium: %v)",
                Config_->Path,
                diskUsage,
                diskLimit,
                Config_->MediumName);

            auto mediumDescriptor = GetMediumDescriptor();
            if (mediumDescriptor.Index != NChunkClient::GenericMediumIndex) {
                DiskResources_.set_usage(diskUsage);
                DiskResources_.set_limit(diskLimit);
                DiskResources_.set_medium_index(mediumDescriptor.Index);
            }
        }
    } catch (const std::exception& ex) {
        auto error = TError("Failed to get disk info") << ex;
        YT_LOG_WARNING(error);
        Disable(error);
    }

    YT_LOG_DEBUG("Disk resources updated");
}

void TSlotLocation::UpdateSlotLocationStatistics()
{
    YT_LOG_DEBUG("Started updating slot location statistics");

    NNodeTrackerClient::NProto::TSlotLocationStatistics slotLocationStatistics;

    {
        auto error = Error_.Load();
        if (!error.IsOK()) {
            ToProto(slotLocationStatistics.mutable_error(), error);
        }
    }

    if (IsEnabled()) {
        try {
            auto locationStatistics = NFS::GetDiskSpaceStatistics(Config_->Path);
            slotLocationStatistics.set_available_space(locationStatistics.AvailableSpace);
            slotLocationStatistics.set_used_space(locationStatistics.TotalSpace - locationStatistics.AvailableSpace);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to get slot location statisitcs")
                << ex;
            YT_LOG_WARNING(error);
            Disable(error);
            return;
        }
    }

    {
        auto guard = WriterGuard(SlotLocationStatisticsLock_);
        SlotLocationStatistics_ = slotLocationStatistics;
    }

    YT_LOG_DEBUG("Slot location statistics updated (UsedSpace: %v, AvailableSpace: %v)",
        slotLocationStatistics.used_space(),
        slotLocationStatistics.available_space());
}

void TSlotLocation::PopulateAlerts(std::vector<TError>* alerts)
{
    auto alert = Alert_.Load();
    if (!alert.IsOK()) {
        alerts->push_back(alert);
    }
}

NNodeTrackerClient::NProto::TDiskLocationResources TSlotLocation::GetDiskResources() const
{
    auto guard = ReaderGuard(DiskResourcesLock_);
    return DiskResources_;
}

void TSlotLocation::AcquireDiskSpace(int slotIndex, i64 diskSpace)
{
    auto guard = WriterGuard(DiskResourcesLock_);

    DiskResources_.set_usage(DiskResources_.usage() + diskSpace);
    EmplaceOrCrash(ReservedDiskSpacePerSlot_, slotIndex, diskSpace);
}

void TSlotLocation::ReleaseDiskSpace(int slotIndex)
{
    auto guard = WriterGuard(DiskResourcesLock_);

    ReservedDiskSpacePerSlot_.erase(slotIndex);
}

NNodeTrackerClient::NProto::TSlotLocationStatistics TSlotLocation::GetSlotLocationStatistics() const
{
    auto guard = ReaderGuard(SlotLocationStatisticsLock_);
    return SlotLocationStatistics_;
}

void TSlotLocation::BuildSlotRootDirectory(int slotIndex)
{
    std::optional<int> uid;
    int nodeUid = getuid();

    if (!Bootstrap_->IsSimpleEnvironment() && !Bootstrap_->GetConfig()->ExecNode->DoNotSetUserId) {
        uid = SlotIndexToUserId_(slotIndex);
    }

    auto directoryBuilderConfig = New<TDirectoryBuilderConfig>();
    directoryBuilderConfig->NodeUid = nodeUid;
    directoryBuilderConfig->NeedRoot = uid.has_value();
    directoryBuilderConfig->RootDirectoryConfigs.push_back(CreateDefaultRootDirectoryConfig(slotIndex, uid, nodeUid));

    RunTool<TRootDirectoryBuilderTool>(directoryBuilderConfig);
}

TRootDirectoryConfigPtr TSlotLocation::CreateDefaultRootDirectoryConfig(
    int slotIndex,
    std::optional<int> uid,
    int nodeUid)
{
    auto config = New<TRootDirectoryConfig>();
    config->SlotPath = GetSlotPath(slotIndex);
    config->UserId = nodeUid;
    config->Permissions = 0755;

    auto getDirectory = [] (TString path, std::optional<int> userId, int permissions) {
        auto directory = New<TDirectoryConfig>();

        directory->Path = path;
        directory->UserId = userId;
        directory->Permissions = permissions;

        return directory;
    };

    // Since we make slot user to be owner, but job proxy creates some files during job shell
    // initialization we leave write access for everybody. Presumably this will not ruin job isolation.
    config->Directories.push_back(getDirectory(GetSandboxPath(slotIndex, ESandboxKind::Home), uid, 0777));

    // Tmp is accessible for everyone.
    config->Directories.push_back(getDirectory(GetSandboxPath(slotIndex, ESandboxKind::Tmp), uid, 0777));

    // CUDA library should have an access to cores directory to write GPU core dump into it.
    config->Directories.push_back(getDirectory(GetSandboxPath(slotIndex, ESandboxKind::Cores), uid, 0777));

    // Pipes are accessible for everyone.
    config->Directories.push_back(getDirectory(GetSandboxPath(slotIndex, ESandboxKind::Pipes), uid, 0777));

    // Node should have access to user sandbox during job preparation.
    config->Directories.push_back(getDirectory(GetSandboxPath(slotIndex, ESandboxKind::User), nodeUid, 0755));

    // Process executor should have access to write logs before process start.
    config->Directories.push_back(getDirectory(GetSandboxPath(slotIndex, ESandboxKind::Logs), uid, 0755));

    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
