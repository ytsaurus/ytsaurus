#include "slot_location.h"
#include "slot_manager.h"
#include "private.h"
#include "config.h"
#include "job_directory_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/master_connector.h>

#include <yt/server/misc/disk_health_checker.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/misc/singleton.h>

#include <yt/core/yson/writer.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/tools/tools.h>

#include <util/system/fs.h>

namespace NYT::NExecAgent {

using namespace NConcurrency;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSlotLocation::TSlotLocation(
    const TSlotLocationConfigPtr& config,
    const NCellNode::TBootstrap* bootstrap,
    const TString& id,
    const IJobDirectoryManagerPtr& jobDirectoryManager,
    bool enableTmpfs)
    : TDiskLocation(config, id, ExecAgentLogger)
    , Config_(config)
    , Bootstrap_(bootstrap)
    , JobDirectoryManager_(jobDirectoryManager)
    , LocationQueue_(New<TActionQueue>(id))
    , EnableTmpfs_(enableTmpfs)
    , HasRootPermissions_(HasRootPermissions())
{
    Enabled_ = true;

    HealthChecker_ = New<TDiskHealthChecker>(
        bootstrap->GetConfig()->DataNode->DiskHealthChecker,
        Config_->Path,
        LocationQueue_->GetInvoker(),
        Logger);

    try {
        NFS::MakeDirRecursive(Config_->Path, 0755);
        HealthChecker_->RunCheck();

        ValidateMinimumSpace();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to initialize slot location %v", Config_->Path) << ex;
        Disable(error);
        return;
    }

    HealthChecker_->SubscribeFailed(BIND(&TSlotLocation::Disable, MakeWeak(this))
        .Via(LocationQueue_->GetInvoker()));
    HealthChecker_->Start();

    DiskInfoUpdateExecutor_ = New<TPeriodicExecutor>(
        LocationQueue_->GetInvoker(),
        BIND(&TSlotLocation::UpdateDiskInfo, MakeWeak(this)),
        Bootstrap_->GetConfig()->ExecAgent->SlotManager->DiskInfoUpdatePeriod);
    DiskInfoUpdateExecutor_->Start();
}

TFuture<std::optional<TString>> TSlotLocation::CreateSandboxDirectories(int slotIndex, TUserSandboxOptions options, int userId)
{
    return BIND([=, this_ = MakeStrong(this)] () -> std::optional<TString> {
         ValidateEnabled();

         LOG_DEBUG("Making sandbox directiories (SlotIndex: %v)", slotIndex);

         auto slotPath = GetSlotPath(slotIndex);
         try {
             NFS::MakeDirRecursive(slotPath, 0755);

             for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
                 auto sandboxPath = GetSandboxPath(slotIndex, sandboxKind);
                 NFS::MakeDirRecursive(sandboxPath, 0700);
             }
         } catch (const std::exception& ex) {
            // Job will be aborted.
             auto error = TError(EErrorCode::SlotLocationDisabled, "Failed to create sandbox directories for slot %v", slotPath)
                << ex;
             Disable(error);
             THROW_ERROR error;
         }

        auto sandboxPath = GetSandboxPath(slotIndex, ESandboxKind::User);

        auto shouldApplyQuota = [&] () {
             bool sandboxTmpfs = false;
             if (options.TmpfsPath) {
                 auto tmpfsPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, *options.TmpfsPath));
                 sandboxTmpfs = (tmpfsPath == sandboxPath);
             }

             return (options.InodeLimit || options.DiskSpaceLimit) && !sandboxTmpfs;
        };

        if (shouldApplyQuota()) {
            try {
                auto properties = TJobDirectoryProperties {options.DiskSpaceLimit, options.InodeLimit, userId};
                WaitFor(JobDirectoryManager_->ApplyQuota(sandboxPath, properties))
                    .ThrowOnError();
                SlotsWithQuota_.insert(slotIndex);
            } catch (const std::exception& ex) {
                auto error = TError(EErrorCode::QuotaSettingFailed, "Failed to set FS quota for a job sandbox")
                    << TErrorAttribute("sandbox_path", sandboxPath)
                    << ex;
                Disable(error);
                THROW_ERROR error;
            }
        }

        // This tmp sandbox is a temporary workaround for nirvana. We apply the same quota as we do for usual sandbox.
        if (options.DiskSpaceLimit ||  options.InodeLimit) {
            auto tmpPath = GetSandboxPath(slotIndex, ESandboxKind::Tmp);
            try {
                auto properties = TJobDirectoryProperties{options.DiskSpaceLimit,  options.InodeLimit, userId};
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
            TWriterGuard guard(SlotsLock_);
            YCHECK(OccupiedSlotToDiskLimit_.emplace(slotIndex, options.DiskSpaceLimit).second);
        }

        if (options.TmpfsPath) {
            auto tmpfsPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, *options.TmpfsPath));
            try {
                if (!tmpfsPath.StartsWith(sandboxPath)) {
                    THROW_ERROR_EXCEPTION("Path of the tmpfs mount point must be inside the sandbox directory")
                        << TErrorAttribute("sandbox_path", sandboxPath)
                        << TErrorAttribute("tmpfs_path", tmpfsPath);
                }

                if (tmpfsPath != sandboxPath) {
                    // If we mount directory inside sandbox, it should not exist.
                    ValidateNotExists(tmpfsPath);
                }

                NFS::MakeDirRecursive(tmpfsPath);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to create directory %Qv for tmpfs in sandbox %v", *options.TmpfsPath, sandboxPath)
                    << ex;
            }

            if (!EnableTmpfs_) {
                return std::nullopt;
            }

            try {
                auto getTmpfsSize = [=] () {
                    YCHECK(options.TmpfsSizeLimit);
                    if (tmpfsPath == sandboxPath && options.DiskSpaceLimit) {
                        return std::min(*options.DiskSpaceLimit, *options.TmpfsSizeLimit);
                    } else {
                        return *options.TmpfsSizeLimit;
                    }
                };

                auto properties = TJobDirectoryProperties{getTmpfsSize(), std::nullopt, userId};
                WaitFor(JobDirectoryManager_->CreateTmpfsDirectory(tmpfsPath, properties))
                    .ThrowOnError();

                YCHECK(TmpfsPaths_.insert(tmpfsPath).second);

                return std::make_optional(tmpfsPath);
            } catch (const std::exception& ex) {
                // Job will be aborted.
                auto error = TError(EErrorCode::SlotLocationDisabled, "Failed to mount tmpfs %v into sandbox %v", *options.TmpfsPath, sandboxPath)
                    << ex;
                Disable(error);
                THROW_ERROR error;
            }
        }

        return std::nullopt;
    })
    .AsyncVia(LocationQueue_->GetInvoker())
    .Run();
}

TFuture<void> TSlotLocation::MakeSandboxCopy(
    int slotIndex,
    ESandboxKind kind,
    const TString& sourcePath,
    const TString& destinationName,
    bool executable)
{
    return BIND([=, this_ = MakeStrong(this)] () {
        ValidateEnabled();

        auto sandboxPath = GetSandboxPath(slotIndex, kind);
        auto destinationPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, destinationName));

        LOG_DEBUG("Making sandbox copy (SourcePath: %v, DestinationName: %v)",
            sourcePath,
            destinationName);

        try {
            // This validations do not disable slot.
            ValidateNotExists(destinationPath);
            ForceSubdirectories(destinationPath, sandboxPath);
        } catch (const std::exception& ex) {
            // Job will be failed.
            THROW_ERROR_EXCEPTION(
                "Failed to make a copy for file %Qv into sandbox %v",
                destinationName,
                sandboxPath)
                    << ex;
        }

        auto logErrorAndDisableLocation = [&] (const std::exception& ex) {
            // Probably location error, job will be aborted.
            auto error = TError(
                EErrorCode::ArtifactCopyingFailed,
                "Failed to make a copy for file %Qv into sandbox %v",
                destinationName,
                sandboxPath) << ex;
            Disable(error);
            THROW_ERROR error;
        };

        try {
            NFS::ChunkedCopy(
                sourcePath,
                destinationPath,
                Bootstrap_->GetConfig()->ExecAgent->SlotManager->FileCopyChunkSize);
            EnsureNotInUse(destinationPath);

            auto permissions = 0666;
            if (executable) {
                permissions |= 0111;
            }
            SetPermissions(destinationPath, permissions);
        } catch (const TErrorException& ex) {
            if (IsInsideTmpfs(destinationPath) && ex.Error().FindMatching(ELinuxErrorCode::NOSPC)) {
                THROW_ERROR_EXCEPTION("Failed to make a copy for file %Qv into sandbox %v: tmpfs is too small",
                    destinationName,
                    sandboxPath) << ex;
            } else if (SlotsWithQuota_.find(slotIndex) != SlotsWithQuota_.end() && ex.Error().FindMatching(ELinuxErrorCode::NOSPC)) {
                THROW_ERROR_EXCEPTION("Failed to make a copy for file %Qv into sandbox %v: disk space limit is too small",
                    destinationName,
                    sandboxPath) << ex;
            } else {
                logErrorAndDisableLocation(ex);
            }
        } catch (const std::exception& ex) {
            logErrorAndDisableLocation(ex);
        }
    })
    .AsyncVia(LocationQueue_->GetInvoker())
    .Run();
}

TFuture<void> TSlotLocation::MakeSandboxLink(
    int slotIndex,
    ESandboxKind kind,
    const TString& targetPath,
    const TString& linkName,
    bool executable)
{
    return BIND([=, this_ = MakeStrong(this)] () {
        ValidateEnabled();

        auto sandboxPath = GetSandboxPath(slotIndex, kind);
        auto linkPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, linkName));

        LOG_DEBUG("Making sandbox symlink (TargetPath: %v, LinkName: %v)", targetPath, linkName);

        try {
            // These validations do not disable slot.
            ValidateNotExists(linkPath);
            ForceSubdirectories(linkPath, sandboxPath);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to make a symlink %Qv into sandbox %v", linkName, sandboxPath)
                << ex;
        }
        
        auto logErrorAndDisableLocation = [&] (const std::exception& ex) {
            // Job will be aborted.
            auto error = TError(EErrorCode::SlotLocationDisabled, "Failed to make a symlink %Qv into sandbox %v", linkName, sandboxPath)
                << ex;
            Disable(error);
            THROW_ERROR error;
        };

        try {
            EnsureNotInUse(targetPath);
            NFS::SetExecutableMode(targetPath, executable);
            NFS::MakeSymbolicLink(targetPath, linkPath);
        } catch (const TErrorException& ex) {
            if (IsInsideTmpfs(targetPath) && ex.Error().FindMatching(ELinuxErrorCode::NOSPC)) {
                THROW_ERROR_EXCEPTION("Failed to make a symlink for file %Qv into sandbox %v: tmpfs is too small",
                    targetPath,
                    sandboxPath) << ex;
            } else if (SlotsWithQuota_.find(slotIndex) != SlotsWithQuota_.end() && ex.Error().FindMatching(ELinuxErrorCode::NOSPC)) {
                THROW_ERROR_EXCEPTION("Failed to make a symlink for file %Qv into sandbox %v: disk space limit is too small",
                    targetPath,
                    sandboxPath) << ex;
            } else {
                logErrorAndDisableLocation(ex);
            }
        } catch (const std::exception& ex) {
            logErrorAndDisableLocation(ex);
        }
    })
    .AsyncVia(LocationQueue_->GetInvoker())
    .Run();
}

TFuture<void> TSlotLocation::FinalizeSanboxPreparation(
    int slotIndex,
    int userId)
{
    return BIND([=, this_ = MakeStrong(this)] () {
        ValidateEnabled();

        auto chownChmod = [&] (ESandboxKind sandboxKind, int permissions) {
            auto sandboxPath = GetSandboxPath(slotIndex, sandboxKind);

            try {
                if (HasRootPermissions_) {
                    auto config = New<TChownChmodConfig>();

                    config->Permissions = permissions;
                    config->Path = sandboxPath;
                    config->UserId = static_cast<uid_t>(userId);
                    RunTool<TChownChmodTool>(config);
                } else {
                    ChownChmodDirectoriesRecursively(sandboxPath, std::nullopt, permissions);
                }
            } catch (const std::exception& ex) {
                auto error = TError(EErrorCode::QuotaSettingFailed, "Failed to set owner and permissions for a job sandbox")
                    << TErrorAttribute("sandbox_path", sandboxPath)
                    << ex;
                Disable(error);
                THROW_ERROR error;
            }
        };

        // We need to give read access to sandbox directory to yt_node/yt_job_proxy effective user (usually yt:yt)
        // and to job user (e.g. yt_slot_N). Since they can have different groups, we fallback to giving read
        // access to everyone.
        // job proxy requires read access e.g. for getting tmpfs size.
        // Write access is for job user only, who becomes an owner.
        chownChmod(ESandboxKind::User, 0755);

        // Since we make slot user to be owner, but job proxy creates some files during job shell
        // initialization we leave write access for everybody. Presumably this will not ruin job isolation.
        chownChmod(ESandboxKind::Home, 0777);

        // Tmp is accessible for everyone.
        chownChmod(ESandboxKind::Tmp, 0777);
    })
    .AsyncVia(LocationQueue_->GetInvoker())
    .Run();
}

TFuture<void> TSlotLocation::MakeConfig(int slotIndex, INodePtr config)
{
    return BIND([=, this_ = MakeStrong(this)] () {
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
            auto error = TError(EErrorCode::SlotLocationDisabled, "Failed to write job proxy config into %v", proxyConfigPath) << ex;
            Disable(error);
            THROW_ERROR error;
        }
    })
    .AsyncVia(LocationQueue_->GetInvoker())
    .Run();
}

TFuture<void> TSlotLocation::CleanSandboxes(int slotIndex)
{
    return BIND([=, this_ = MakeStrong(this)] () {
        ValidateEnabled();

        {
            TWriterGuard guard(SlotsLock_);

            // There may be no slotIndex in this map
            // (e.g. during SlotMananager::Initialize)
            OccupiedSlotToDiskLimit_.erase(slotIndex);
        }

        for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
            const auto& sandboxPath = NFS::GetRealPath(GetSandboxPath(slotIndex, sandboxKind));
            try {
                if (!NFS::Exists(sandboxPath)) {
                    continue;
                }

                LOG_DEBUG("Removing job directories (Path: %v)", sandboxPath);

                WaitFor(JobDirectoryManager_->CleanDirectories(sandboxPath))
                    .ThrowOnError();

                LOG_DEBUG("Cleaning sandbox directory (Path: %v)", sandboxPath);

                if (HasRootPermissions_) {
                    RunTool<TRemoveDirAsRootTool>(sandboxPath);
                } else {
                    NFS::RemoveRecursive(sandboxPath);
                }

                auto it = TmpfsPaths_.lower_bound(sandboxPath);
                while (it != TmpfsPaths_.end() && it->StartsWith(sandboxPath)) {
                    it = TmpfsPaths_.erase(it);
                }

                SlotsWithQuota_.erase(slotIndex);
            } catch (const std::exception& ex) {
                auto error = TError("Failed to clean sandbox directory %v", sandboxPath) << ex;
                Disable(error);
                THROW_ERROR error;
            }
        }
    })
    .AsyncVia(LocationQueue_->GetInvoker())
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
    return NFS::CombinePaths(Config_->Path, Format("%v", slotIndex));
}

TString TSlotLocation::GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    Y_ASSERT(sandboxName);
    return NFS::CombinePaths(GetSlotPath(slotIndex), sandboxName);
}

bool TSlotLocation::IsInsideTmpfs(const TString& path) const
{
    auto it = TmpfsPaths_.lower_bound(path);
    if (it != TmpfsPaths_.begin()) {
        --it;
        if (path.StartsWith(*it + "/")) {
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
    if (!Enabled_.exchange(false))
        return;

    auto alert = TError(
        EErrorCode::SlotLocationDisabled,
        "Slot location at %v is disabled",
        Config_->Path) << error;

    LOG_ERROR(alert);

    auto masterConnector = Bootstrap_->GetMasterConnector();
    masterConnector->RegisterAlert(alert);

    if (DiskInfoUpdateExecutor_) {
        DiskInfoUpdateExecutor_->Stop();
    }
}

void TSlotLocation::UpdateDiskInfo()
{
    if (!IsEnabled()) {
        return;
    }

    try {
        auto locationStatistics = NFS::GetDiskSpaceStatistics(Config_->Path);
        i64 diskLimit = locationStatistics.TotalSpace;
        if (Config_->DiskQuota) {
            diskLimit = Min(diskLimit, *Config_->DiskQuota);
        }

        i64 diskUsage = 0;
        THashMap<int, std::optional<i64>> occupiedSlotToDiskLimit;

        {
            TReaderGuard guard(SlotsLock_);
            occupiedSlotToDiskLimit = OccupiedSlotToDiskLimit_;
        }

        for (const auto& pair : occupiedSlotToDiskLimit) {
            auto slotIndex = pair.first;
            const auto& slotDiskLimit = pair.second;
            if (!slotDiskLimit) {
                for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
                    auto path = GetSandboxPath(slotIndex, sandboxKind);
                    if (NFS::Exists(path)) {
                        // We have to calculate user directory size as root,
                        // because user job could have set restricted permissions for files and
                        // directories inside sandbox.
                        auto dirSize = (sandboxKind == ESandboxKind::User) && HasRootPermissions_
                            ? RunTool<TGetDirectorySizeAsRootTool>(path)
                            : NFS::GetDirectorySize(path);
                        diskUsage += dirSize;
                    }
                }
            } else {
                diskUsage += *slotDiskLimit;
            }
        }

        auto availableSpace = Max<i64>(0, Min(locationStatistics.AvailableSpace, diskLimit - diskUsage));
        diskLimit = Min(diskLimit, diskUsage + availableSpace);

        diskLimit -= Config_->DiskUsageWatermark;

        LOG_DEBUG("Disk info (Path: %v, Usage: %v, Limit: %v)",
            Config_->Path,
            diskUsage,
            diskLimit);

        {
            auto guard = TWriterGuard(DiskInfoLock_);
            DiskInfo_.set_usage(diskUsage);
            DiskInfo_.set_limit(diskLimit);
        }
    } catch (const std::exception& ex) {
        auto error = TError("Failed to get disk info") << ex;
        LOG_WARNING(error);
        Disable(error);
    }
}

NNodeTrackerClient::NProto::TDiskResourcesInfo TSlotLocation::GetDiskInfo() const
{
    auto guard = TReaderGuard(DiskInfoLock_);
    return DiskInfo_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
