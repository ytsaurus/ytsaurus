#include "slot_location.h"
#include "private.h"
#include "config.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/master_connector.h>

#include <yt/server/misc/disk_health_checker.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/yson/writer.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/tools/tools.h>

#include <util/system/fs.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSlotLocation::TSlotLocation(
    const TSlotLocationConfigPtr& config,
    const NCellNode::TBootstrap* bootstrap,
    const Stroka& id,
    IInvokerPtr invoker)
    : TDiskLocation(config, id, ExecAgentLogger)
    , Config_(config)
    , Bootstrap_(bootstrap)
    , HasRootPermissions_(HasRootPermissions())
    , Invoker_(std::move(invoker))
{
    Enabled_ = true;

    HealthChecker_ = New<TDiskHealthChecker>(
        bootstrap->GetConfig()->DataNode->DiskHealthChecker,
        Config_->Path,
        Invoker_,
        Logger);

    try {
        NFS::ForcePath(Config_->Path, 0755);
        HealthChecker_->RunCheck();

        ValidateMinimumSpace();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to initialize slot location %v", Config_->Path) << ex;
        Disable(error);
        return;
    }

    HealthChecker_->SubscribeFailed(BIND(&TSlotLocation::Disable, MakeWeak(this))
        .Via(Invoker_));
    HealthChecker_->Start();
}

void TSlotLocation::CreateSandboxDirectories(int slotIndex)
{
    WaitFor(
        BIND([=, this_ = MakeStrong(this)] () {
             ValidateEnabled();

             LOG_DEBUG("Making sandbox directiories (SlotIndex: %v)", slotIndex);

             auto slotPath = GetSlotPath(slotIndex);
             try {
                 NFS::ForcePath(slotPath, 0755);

                 for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
                     auto sandboxPath = GetSandboxPath(slotIndex, sandboxKind);
                     NFS::ForcePath(sandboxPath, 0777);
                 }
             } catch (const std::exception& ex) {
                 auto error = TError("Failed to create sandbox directories for slot %v", slotPath) 
                    << ex;
                 Disable(error);
                 THROW_ERROR error;
             }
        })
        .AsyncVia(Invoker_)
        .Run())
    .ThrowOnError();
}

void TSlotLocation::MakeSandboxCopy(
    int slotIndex,
    ESandboxKind kind,
    const Stroka& sourcePath,
    const Stroka& destinationName,
    bool executable)
{
    WaitFor(
        BIND([=, this_ = MakeStrong(this)] () {
            ValidateEnabled();

            auto sandboxPath = GetSandboxPath(slotIndex, kind);
            auto destinationPath = NFS::CombinePaths(sandboxPath, destinationName);

            LOG_DEBUG("Making sandbox copy (SourcePath: %v, DestinationName: %v)", sourcePath, destinationName);
            
            try {
                // This validations do not disable slot.
                ValidateNotExists(destinationPath);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to make a copy for file %Qv into sandbox %v", destinationName, sandboxPath) << ex;
            }

            try {
                EnsureNotInUse(sourcePath);
                NFS::SetExecutableMode(sourcePath, executable);
                NFs::Copy(sourcePath, destinationPath);
            } catch (const std::exception& ex) {
                try {
                    // If tmpfs does not have enough space, this is a user error, not location error. 
                    // Let's check it first, before disabling location.

                    const auto& systemError = dynamic_cast<const TSystemError&>(ex);
                    if (IsInsideTmpfs(destinationPath) && systemError.Status() == ENOSPC) {
                        THROW_ERROR_EXCEPTION("Failed to make a copy for file %Qv into sandbox %v: tmpfs is too small", destinationName, sandboxPath) 
                            << ex;
                    }
                } catch (const std::bad_cast& badCast) {
                    // Just do nothing, this is not a TSystemError.
                }

                auto error = TError("Failed to make a copy for file %Qv into sandbox %v", destinationName, sandboxPath) << ex;
                Disable(error);
                THROW_ERROR error;
            }
        })
        .AsyncVia(Invoker_)
        .Run())
    .ThrowOnError();
}

void TSlotLocation::MakeSandboxLink(
    int slotIndex,
    ESandboxKind kind,
    const Stroka& targetPath,
    const Stroka& linkName,
    bool executable)
{
    WaitFor(
        BIND([=, this_ = MakeStrong(this)] () {
            ValidateEnabled();

            auto sandboxPath = GetSandboxPath(slotIndex, kind);
            auto linkPath = NFS::CombinePaths(sandboxPath, linkName);

            LOG_DEBUG("Making sandbox symlink (TargetPath: %v, LinkName: %v)", targetPath, linkName);

            try {
                // This validations do not disable slot.
                ValidateNotExists(linkPath);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to make a symlink %Qv into sandbox %v", linkName, sandboxPath) 
                    << ex;
            }

            try {
                EnsureNotInUse(targetPath);
                NFS::SetExecutableMode(targetPath, executable);
                NFS::MakeSymbolicLink(targetPath, linkPath);
            } catch (const std::exception& ex) {
                auto error = TError("Failed to make a symlink %Qv into sandbox %v", linkName, sandboxPath) 
                    << ex;
                Disable(error);
                THROW_ERROR error;
            }
        })
        .AsyncVia(Invoker_)
        .Run())
    .ThrowOnError();
}

Stroka TSlotLocation::MakeSandboxTmpfs(
    int slotIndex,
    ESandboxKind kind,
    i64 size,
    int userId,
    const Stroka& path)
{
    return WaitFor(
        BIND([=, this_ = MakeStrong(this)] () {
            ValidateEnabled();
            auto sandboxPath = GetSandboxPath(slotIndex, kind);
            auto tmpfsPath = NFS::GetRealPath(NFS::CombinePaths(sandboxPath, path));
            auto isSandbox = tmpfsPath == sandboxPath;

            try {
                // This validations do not disable slot.
                if (!HasRootPermissions_) {
                    THROW_ERROR_EXCEPTION("Sandbox tmpfs in disabled since node doesn't have root permissions");
                }

                if (!sandboxPath.is_prefix(tmpfsPath)) {
                    THROW_ERROR_EXCEPTION("Path of the tmpfs mount point must be inside the sandbox directory")
                        << TErrorAttribute("sandbox_path", sandboxPath)
                        << TErrorAttribute("tmpfs_path", tmpfsPath);
                }

                if (!isSandbox) {
                    // If we mount directory inside sandbox, it should not exist.
                    ValidateNotExists(tmpfsPath);
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to mount tmpfs %v into sandbox %v", path, sandboxPath)
                    << ex;
            }

            try {
                auto config = New<TMountTmpfsConfig>();
                config->Path = tmpfsPath;
                config->Size = size;

                // If we mount the whole sandbox, we use current process uid instead of slot one.
                config->UserId = isSandbox ? ::geteuid() : userId;

                LOG_DEBUG("Mounting tmpfs %v", ConvertToYsonString(config, EYsonFormat::Text));

                NFS::ForcePath(tmpfsPath);
                RunTool<TMountTmpfsAsRootTool>(config);
                if (isSandbox) {
                    // We must give user full access to his sandbox.
                    NFS::Chmod(tmpfsPath, 0777);
                }

                TmpfsPaths_.insert(tmpfsPath);
                return tmpfsPath;
            } catch (const std::exception& ex) {
                auto error = TError("Failed to mount tmpfs %v into sandbox %v", path, sandboxPath) 
                    << ex;
                Disable(error);
                THROW_ERROR error;
            }
        })
        .AsyncVia(Invoker_)
        .Run())
    .ValueOrThrow();
}

void TSlotLocation::MakeConfig(int slotIndex, INodePtr config)
{
    WaitFor(
        BIND([=, this_ = MakeStrong(this)] () {
            ValidateEnabled();
            auto proxyConfigPath = GetConfigPath(slotIndex);

            try {
                TFile file(proxyConfigPath, CreateAlways | WrOnly | Seq | CloseOnExec);
                TFileOutput output(file);
                TYsonWriter writer(&output, EYsonFormat::Pretty);
                Serialize(config, &writer);
                writer.Flush();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to write job proxy config into %v", proxyConfigPath) << ex;
                Disable(error);
                THROW_ERROR error;
            }
        })
        .AsyncVia(Invoker_)
        .Run())
    .ThrowOnError();
}

void TSlotLocation::CleanSandboxes(int slotIndex)
{
    WaitFor(
        BIND([=, this_ = MakeStrong(this)] () {
            ValidateEnabled();

            for (auto sandboxKind : TEnumTraits<ESandboxKind>::GetDomainValues()) {
                const auto& sandboxPath = GetSandboxPath(slotIndex, sandboxKind);
                try {
                    auto sandboxFullPath = NFS::CombinePaths(~NFs::CurrentWorkingDirectory(), sandboxPath);

                    // Unmount all known tmpfs.
                    std::vector<Stroka> tmpfsPaths;
                    for (const auto& tmpfsPath : TmpfsPaths_) {
                        if (sandboxFullPath.is_prefix(tmpfsPath)) {
                            tmpfsPaths.push_back(tmpfsPath);
                        }
                    }

                    for (const auto& path : tmpfsPaths) {
                        TmpfsPaths_.erase(path);
                        RunTool<TRemoveDirAsRootTool>(path + "/*");
                        RunTool<TUmountAsRootTool>(path);
                    }

                    // Unmount unknown tmpfs, e.g. left from previous node run.

                    // NB: iterating over /proc/mounts is not reliable,
                    // see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=593516.
                    // To avoid problems with undeleting tmpfs ordered by user in sandbox
                    // we always try to remove it several times.
                    for (int attempt = 0; attempt < TmpfsRemoveAttemptCount; ++attempt) {
                        // Look mount points inside sandbox and unmount it.
                        auto mountPoints = NFS::GetMountPoints();
                        for (const auto& mountPoint : mountPoints) {
                            if (sandboxFullPath.is_prefix(mountPoint.Path)) {
                                // '/*' added since we need to remove only content.
                                RunTool<TRemoveDirAsRootTool>(mountPoint.Path + "/*");
                                RunTool<TUmountAsRootTool>(mountPoint.Path);
                            }
                        }
                    }

                    if (NFS::Exists(sandboxPath)) {
                        LOG_DEBUG("Cleaning sandbox directory (Path: %v)", sandboxPath);
                        if (HasRootPermissions_) {
                            RunTool<TRemoveDirAsRootTool>(sandboxPath);
                        } else {
                            NFS::RemoveRecursive(sandboxPath);
                        }
                    }
                } catch (const std::exception& ex) {
                    auto error = TError("Failed to clean sandbox directory %v", sandboxPath) << ex;
                    Disable(error);
                    THROW_ERROR error;
                }
            }
        })
        .AsyncVia(Invoker_)
        .Run())
    .ThrowOnError();
}

void TSlotLocation::IncreaseSessionCount()
{
    ++SessionCount_;
}

void TSlotLocation::DecreaseSessionCount()
{
    --SessionCount_;
}

void TSlotLocation::ValidateNotExists(const Stroka& path) const
{
    if (NFS::Exists(path)) {
        THROW_ERROR_EXCEPTION("Path %v already exists", path);
    }
}

void TSlotLocation::EnsureNotInUse(const Stroka& path) const
{
    // Take exclusive lock in blocking fashion to ensure that no
    // forked process is holding an open descriptor to the source file.
    TFile file(path, RdOnly | CloseOnExec);
    file.Flock(LOCK_EX);
}

Stroka TSlotLocation::GetConfigPath(int slotIndex) const
{
    return NFS::CombinePaths(GetSlotPath(slotIndex), ProxyConfigFileName);
}

Stroka TSlotLocation::GetSlotPath(int slotIndex) const
{
    return NFS::CombinePaths(Config_->Path, Format("%v", slotIndex));
}

Stroka TSlotLocation::GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const
{
    const auto& sandboxName = SandboxDirectoryNames[sandboxKind];
    Y_ASSERT(sandboxName);
    return NFS::CombinePaths(GetSlotPath(slotIndex), sandboxName);
}

bool TSlotLocation::IsInsideTmpfs(const Stroka& path) const
{
    for (const auto& tmpfsPath : TmpfsPaths_) {
        if (tmpfsPath.is_prefix(path)) {
            return true;
        }
    }

    return false;
}

void TSlotLocation::Disable(const TError& error)
{
    if (!Enabled_.exchange(false))
        return;

    auto alert = TError("Slot location at %v is disabled", Config_->Path) << error;

    LOG_ERROR(alert);

    auto masterConnector = Bootstrap_->GetMasterConnector();
    masterConnector->RegisterAlert(alert);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
