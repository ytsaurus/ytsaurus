#include "proc.h"

#include <yt/core/misc/proc.h>
#include <yt/core/misc/fs.h>

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
    SafeSetUid(0);
    execl("/bin/rm", "/bin/rm", "-rf", path.c_str(), (void*)nullptr);

    THROW_ERROR_EXCEPTION("Failed to remove directory %v: execl failed",
        path) << TError::FromSystem();
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
        error.InnerErrors() = std::move(innerErrors);

        attemptErrors.push_back(error);

        Sleep(TDuration::Seconds(1));
    }

    if (!removed) {
        auto error = TError("Failed to remove directory %v contents", path);
        error.InnerErrors() = std::move(attemptErrors);
        THROW_ERROR error;
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

i64 TGetDirectorySizeAsRootTool::operator()(const TString& path) const
{
    SafeSetUid(0);
    return NFS::GetDirectorySize(path);
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

} // namespace NYT::NTools
