#include "proc.h"
#include "seccomp.h"

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/fs.h>

#include <util/system/yield.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>

#include <util/folder/iterator.h>

namespace NYT::NTools {

using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

void TKillAllByUidTool::operator()(int uid) const
{
    SafeSetUid(0);

    auto pidsToKill = GetPidsByUid(uid);
    if (pidsToKill.empty()) {
        return;
    }

    while (true) {
        for (int pid : pidsToKill) {
            auto result = kill(pid, 9);
            if (result == -1) {
                YT_VERIFY(errno == ESRCH);
            }
        }

        pidsToKill = GetPidsByUid(uid);
        if (pidsToKill.empty())
            break;

        ThreadYield();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveDirAsRootTool::operator()(const TString& path) const
{
    // Child process
    TrySetUid(0);
    execl("/bin/rm", "/bin/rm", "-rf", path.c_str(), (void*)nullptr);

    THROW_ERROR_EXCEPTION("Failed to remove directory %v: execl failed",
        path) << TError::FromSystem();
}

////////////////////////////////////////////////////////////////////////////////

void TCreateDirectoryAsRootTool::operator()(const TString& path) const
{
    // Child process
    TrySetUid(0);
    NFS::MakeDirRecursive(path);
}

////////////////////////////////////////////////////////////////////////////////

void TSpawnShellTool::operator()(TSpawnShellConfigPtr config) const
{
    SetupSeccomp();

    if (config->Command) {
        execl("/bin/bash", "/bin/bash", "-c",  config->Command->c_str(), (void*)nullptr);
    } else {
        execl("/bin/bash", "/bin/bash", (void*)nullptr);
    }

    THROW_ERROR_EXCEPTION("Failed to spawn job shell")
        << TError::FromSystem();
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveDirContentAsRootTool::operator()(const TString& path) const
{
    // Child process
    SafeSetUid(0);

    if (!TFileStat(path).IsDir()) {
        THROW_ERROR_EXCEPTION("Path %v is not directory",
            path);
    }

    auto isRemovable = [&] (auto it) {
        if (it->fts_info == FTS_DOT || it->fts_info == FTS_D) {
            return false;
        }
        if (path.StartsWith(it->fts_path)) {
            return false;
        }

        return true;
    };

    bool removed = false;
    std::vector<TError> attemptErrors;

    constexpr int RemoveAsRootAttemptCount = 5;
    for (int attempt = 0; attempt < RemoveAsRootAttemptCount; ++attempt) {
        std::vector<TError> innerErrors;
        {
            TDirIterator dir(path);
            for (auto it = dir.begin(); it != dir.end(); ++it) {
                try {
                    if (isRemovable(it)) {
                        NFS::Remove(it->fts_path);
                    }
                } catch (const std::exception& ex) {
                    innerErrors.push_back(TError("Failed to remove path %v", it->fts_path)
                        << ex);
                }
            }
        }

        std::vector<TString> unremovableItems;
        {
            TDirIterator dir(path);
            for (auto it = dir.begin(); it != dir.end(); ++it) {
                if (isRemovable(it)) {
                    unremovableItems.push_back(it->fts_path);
                }
            }
        }

        if (unremovableItems.empty()) {
            removed = true;
            break;
        }

        auto error = TError("Failed to remove items %v in directory %v",
            unremovableItems,
            path);

        error = NFS::AttachLsofOutput(error, path);
        error = NFS::AttachFindOutput(error, path);
        *error.MutableInnerErrors() = std::move(innerErrors);

        attemptErrors.push_back(error);

        Sleep(TDuration::Seconds(1));
    }

    if (!removed) {
        THROW_ERROR_EXCEPTION("Failed to remove directory %v contents", path)
            << attemptErrors;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMountTmpfsAsRootTool::operator()(TMountTmpfsConfigPtr config) const
{
    SafeSetUid(0);
    NFS::MountTmpfs(config->Path, config->UserId, config->Size);
}

////////////////////////////////////////////////////////////////////////////////

void TUmountAsRootTool::operator()(TUmountConfigPtr config) const
{
    SafeSetUid(0);
    NFS::Umount(config->Path, config->Detach);
}

////////////////////////////////////////////////////////////////////////////////

void TSetThreadPriorityAsRootTool::operator()(TSetThreadPriorityConfigPtr config) const
{
    SafeSetUid(0);
    SetThreadPriority(config->ThreadId, config->Priority);
}

////////////////////////////////////////////////////////////////////////////////

void TFSQuotaTool::operator()(TFSQuotaConfigPtr config) const
{
    SafeSetUid(0);
    NFS::SetQuota(config->UserId, config->Path, config->DiskSpaceLimit, config->InodeLimit);
}

////////////////////////////////////////////////////////////////////////////////

void TChownChmodTool::operator()(TChownChmodConfigPtr config) const
{
    SafeSetUid(0);
    ChownChmodDirectoriesRecursively(config->Path, config->UserId, config->Permissions);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<i64> TGetDirectorySizesAsRootTool::operator()(const TGetDirectorySizesAsRootConfigPtr& config) const
{
    TrySetUid(0);

    const auto& paths = config->Paths;

    std::vector<i64> sizes;
    sizes.reserve(paths.size());
    for (const auto& path : paths) {
        auto size = NFS::GetDirectorySize(path, config->IgnoreUnavailableFiles, config->DeduplicateByINodes, config->CheckDeviceId);
        sizes.push_back(size);
    }

    return sizes;
}

////////////////////////////////////////////////////////////////////////////////

void TCopyDirectoryContentTool::operator()(TCopyDirectoryContentConfigPtr config) const
{
    SafeSetUid(0);

    execl("/usr/bin/rsync", "/usr/bin/rsync", "-q", "--perms", "--recursive", "--specials", "--links", config->Source.c_str(), config->Destination.c_str(), (void*)nullptr);

    THROW_ERROR_EXCEPTION("Failed to copy directory %Qv to %Qv: execl failed",
        config->Source,
        config->Destination)
        << TError::FromSystem();
}

////////////////////////////////////////////////////////////////////////////////

TString TReadProcessSmapsTool::operator()(int pid) const
{
    SafeSetUid(0);
    return TFileInput{Format("/proc/%v/smaps", pid)}.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

void TMountTmpfsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("user_id", &TThis::UserId)
        .GreaterThanOrEqual(0);
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
}

////////////////////////////////////////////////////////////////////////////////

void TSpawnShellConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("command", &TThis::Command)
        .Default(std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

void TUmountConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("detach", &TThis::Detach);
}

////////////////////////////////////////////////////////////////////////////////

void TSetThreadPriorityConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_id", &TThis::ThreadId);
    registrar.Parameter("priority", &TThis::Priority);
}

////////////////////////////////////////////////////////////////////////////////

void TFSQuotaConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("disk_space_limit", &TThis::DiskSpaceLimit)
        .GreaterThanOrEqual(0)
        .Default();
    registrar.Parameter("inode_limit", &TThis::InodeLimit)
        .GreaterThanOrEqual(0)
        .Default();
    registrar.Parameter("user_id", &TThis::UserId)
        .GreaterThanOrEqual(0);
    registrar.Parameter("path", &TThis::Path);
}

////////////////////////////////////////////////////////////////////////////////

void TChownChmodConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .NonEmpty();
    registrar.Parameter("user_id", &TThis::UserId)
        .Default();
    registrar.Parameter("permissions", &TThis::Permissions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TGetDirectorySizesAsRootConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("paths", &TThis::Paths)
        .Default();
    registrar.Parameter("ignore_unavailable_files", &TThis::IgnoreUnavailableFiles)
        .Default(true);
    registrar.Parameter("deduplicate_by_inodes", &TThis::DeduplicateByINodes)
        .Default(false);
    registrar.Parameter("check_device_id", &TThis::CheckDeviceId)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TCopyDirectoryContentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("source", &TThis::Source)
        .NonEmpty();
    registrar.Parameter("destination", &TThis::Destination)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDirectoryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default();
    registrar.Parameter("user_id", &TThis::UserId)
        .Default();
    registrar.Parameter("permissions", &TThis::Permissions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TRootDirectoryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_path", &TThis::SlotPath)
        .Default();
    registrar.Parameter("user_id", &TThis::UserId)
        .Default();
    registrar.Parameter("permissions", &TThis::Permissions)
        .Default();
    registrar.Parameter("directories", &TThis::Directories)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDirectoryBuilderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("need_root", &TThis::NeedRoot)
        .Default(false);
    registrar.Parameter("node_uid", &TThis::NodeUid)
        .Default();
    registrar.Parameter("slot_configs", &TThis::RootDirectoryConfigs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TRootDirectoryBuilderTool::operator()(const TDirectoryBuilderConfigPtr& arg) const
{
    if (arg->NeedRoot) {
        TrySetUid(0);
    }

    for (const auto& rootDirectoryConfig : arg->RootDirectoryConfigs) {
        // Create slot directory if it does not exist.
        NFS::MakeDirRecursive(
            rootDirectoryConfig->SlotPath,
            rootDirectoryConfig->Permissions);

        ChownChmodDirectory(
            rootDirectoryConfig->SlotPath,
            rootDirectoryConfig->UserId,
            rootDirectoryConfig->Permissions);

        for (const auto& directory : rootDirectoryConfig->Directories) {
            if (NFS::Exists(directory->Path)) {
                NFS::RemoveRecursive(directory->Path);
            }
            NFs::MakeDirectory(
                directory->Path,
                NFs::EFilePermission::FP_SECRET_FILE);
        }

        for (const auto& directory : rootDirectoryConfig->Directories) {
            ChownChmodDirectory(
                directory->Path,
                directory->UserId,
                directory->Permissions);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
