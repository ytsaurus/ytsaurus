#include "stdafx.h"
#include "slot.h"
#include "private.h"
#include "config.h"

#include <ytlib/cgroup/cgroup.h>

#include <core/concurrency/action_queue.h>

#include <core/tools/tools.h>

#include <core/misc/proc.h>

#include <core/logging/log_manager.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

Stroka GetSlotProcessGroup(int slotId)
{
    return "slot" + ToString(slotId);
}

////////////////////////////////////////////////////////////////////////////////

TSlot::TSlot(
    TSlotManagerConfigPtr config,
    std::vector<Stroka> paths,
    const Stroka& nodeId,
    IInvokerPtr invoker,
    int slotIndex,
    TNullable<int> userId)
    : Paths_(std::move(paths))
    , NodeId_(nodeId)
    , SlotIndex_(slotIndex)
    , UserId_(userId)
    , Invoker_(std::move(invoker))
    , ProcessGroup_("freezer", GetSlotProcessGroup(slotIndex))
    , NullCGroup_()
    , Logger(ExecAgentLogger)
    , Config_(config)
{
    Logger.AddTag("Slot: %v", SlotIndex_);
}

void TSlot::Initialize()
{
    if (Config_->EnableCGroups) {
        try {
            ProcessGroup_.EnsureExistance();
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to create process group %Qv",
                ProcessGroup_.GetFullPath());
        }

#ifdef _linux_
        try {
            NCGroup::RunKiller(ProcessGroup_.GetFullPath());
        } catch (const std::exception& ex) {
            // ToDo(psushin): think about more complex logic of handling fs errors.
            LOG_FATAL(ex, "Failed to clean process group %v",
                ProcessGroup_.GetFullPath());
        }
#endif

        ProcessGroup_.Unlock();
    }

    Stroka currentPath;
    try {
        for (int pathIndex = 0; pathIndex < Paths_.size(); ++pathIndex) {
            const auto& path = Paths_[pathIndex];
            currentPath = path;
            NFS::ForcePath(path, 0755);
            SandboxPaths_.emplace_back();
            for (auto sandboxIndex : TEnumTraits<ESandboxIndex>::GetDomainValues()) {
                const auto& sandboxName = SandboxDirectoryNames[sandboxIndex];
                YASSERT(sandboxName);
                SandboxPaths_[pathIndex][sandboxIndex] = NFS::CombinePaths(path, sandboxName);
            }
            DoCleanSandbox(pathIndex);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create slot directory %v",
            currentPath) << ex;
    }

    try {
        DoCleanProcessGroups();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to clean slot cgroups")
            << ex;
    }
}

void TSlot::Acquire(int pathIndex)
{
    YCHECK(pathIndex >= 0 && pathIndex < Paths_.size());

    PathIndex_ = pathIndex;
    IsFree_.store(false);
}

bool TSlot::IsFree() const
{
    return IsFree_.load();
}

TNullable<int> TSlot::GetUserId() const
{
    return UserId_;
}

const NCGroup::TNonOwningCGroup& TSlot::GetProcessGroup() const
{
    return Config_->EnableCGroups ? ProcessGroup_ : NullCGroup_;
}

std::vector<Stroka> TSlot::GetCGroupPaths() const
{
    std::vector<Stroka> result;
    if (Config_->EnableCGroups) {
        auto subgroupName = GetSlotProcessGroup(SlotIndex_);

        for (const auto& type : Config_->SupportedCGroups) {
            NCGroup::TNonOwningCGroup group(type, subgroupName);
            result.push_back(group.GetFullPath());
        }
        result.push_back(ProcessGroup_.GetFullPath());
    }
    return result;
}

int TSlot::GetPathIndex() const
{
    return PathIndex_;
}

TTcpBusServerConfigPtr TSlot::GetRpcServerConfig() const
{
    auto unixDomainName = Format("%v-job-proxy-%v", NodeId_, SlotIndex_);
    return TTcpBusServerConfig::CreateUnixDomain(unixDomainName);
}

TTcpBusClientConfigPtr TSlot::GetRpcClientConfig() const
{
    auto unixDomainName = Format("%v-job-proxy-%v", NodeId_, SlotIndex_);
    return TTcpBusClientConfig::CreateUnixDomain(unixDomainName);
}

void TSlot::DoCleanSandbox(int pathIndex)
{
    for (auto sandboxIndex : TEnumTraits<ESandboxIndex>::GetDomainValues()) {
        const auto& sandboxPath = SandboxPaths_[pathIndex][sandboxIndex];
        try {
            if (NFS::Exists(sandboxPath)) {
                if (UserId_) {
                    LOG_DEBUG("Clean sandbox %v", sandboxPath);
                    RunTool<TRemoveDirAsRootTool>(sandboxPath);
                } else {
                    NFS::RemoveRecursive(sandboxPath);
                }
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Failed to clean sandbox directory %v",
                sandboxPath)
                << ex;
            LOG_ERROR(wrappedError);
            THROW_ERROR wrappedError;
        }
    }
}

void TSlot::DoCleanProcessGroups()
{
    if (!Config_->EnableCGroups) {
        return;
    }

    try {
        for (const auto& path : GetCGroupPaths()) {
            NCGroup::TNonOwningCGroup group(path);
            group.RemoveRecursive();
        }
        ProcessGroup_.EnsureExistance();
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Failed to clean slot subcgroups for slot %v",
            SlotIndex_) << ex;
        LOG_ERROR(wrappedError);
        THROW_ERROR wrappedError;
    }
}

void TSlot::Clean()
{
    YCHECK(!IsFree());
    try {
        LOG_INFO("Cleaning slot");
        DoCleanProcessGroups();
        DoCleanSandbox(PathIndex_);
        IsClean_ = true;
    } catch (const std::exception& ex) {
        LOG_FATAL("%v", ex.what());
    }
}

void TSlot::Release()
{
    YCHECK(IsClean_);

    if (Config_->EnableCGroups) {
        ProcessGroup_.Unlock();
    }

    IsFree_.store(true);
    PathIndex_ = -1;
}

void TSlot::InitSandbox()
{
    YCHECK(!IsFree());

    for (auto sandboxIndex : TEnumTraits<ESandboxIndex>::GetDomainValues()) {
        const auto& sandboxPath = SandboxPaths_[PathIndex_][sandboxIndex];
        try {
            NFS::ForcePath(sandboxPath, 0777);
        } catch (const std::exception& ex) {
            LogErrorAndExit(TError("Failed to create sandbox directory %Qv",
                sandboxPath)
                << ex);
        }
        LOG_INFO("Created slot sandbox directory %Qv", sandboxPath);
    }

    IsClean_ = false;
}

void TSlot::MakeLink(
    ESandboxIndex sandboxIndex,
    const Stroka& targetPath,
    const Stroka& linkName,
    bool isExecutable) noexcept
{
    YCHECK(!IsFree());

    const auto& sandboxPath = SandboxPaths_[PathIndex_][sandboxIndex];
    auto linkPath = NFS::CombinePaths(sandboxPath, linkName);
    try {
        {
            // Take exclusive lock in blocking fashion to ensure that no
            // forked process is holding an open descriptor to the target file.
            TFile file(targetPath, RdOnly | CloseOnExec);
            file.Flock(LOCK_EX);
        }

        NFS::MakeSymbolicLink(targetPath, linkPath);
        NFS::SetExecutableMode(linkPath, isExecutable);
    } catch (const std::exception& ex) {
        // Occured IO error in the slot, restart node immediately.
        LogErrorAndExit(TError(
            "Failed to create a symlink in the slot %Qv (LinkPath: %Qv, TargetPath: %Qv, IsExecutable: %lv)",
            sandboxPath,
            linkPath,
            targetPath,
            isExecutable)
            << ex);
    }
}

void TSlot::LogErrorAndExit(const TError& error)
{
    LOG_ERROR(error);
    NLogging::TLogManager::Get()->Shutdown();
    _exit(1);
}

const Stroka& TSlot::GetWorkingDirectory() const
{
    YCHECK(!IsFree());
    return Paths_[PathIndex_];
}

IInvokerPtr TSlot::GetInvoker()
{
    return Invoker_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
