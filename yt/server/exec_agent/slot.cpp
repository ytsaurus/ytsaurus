#include "stdafx.h"
#include "slot.h"
#include "private.h"
#include "config.h"

#include <ytlib/cgroup/cgroup.h>

#include <core/misc/proc.h>

#include <core/ytree/yson_producer.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

Stroka GetSlotProcessGroup(int slotId)
{
    return "slot" + ToString(slotId);
}

////////////////////////////////////////////////////////////////////////////////

TSlot::TSlot(
    TSlotManagerConfigPtr config,
    const Stroka& path,
    int slotIndex,
    int userId)
    : IsFree_(true)
    , IsClean(true)
    , Path(path)
    , SlotIndex(slotIndex)
    , UserId(userId)
    , SlotThread(New<TActionQueue>(Format("ExecSlot:%v", slotIndex)))
    , ProcessGroup("freezer", GetSlotProcessGroup(slotIndex))
    , NullCGroup()
    , Logger(ExecAgentLogger)
    , Config(config)
{
    Logger.AddTag("Slot: %v", SlotIndex);
}

void TSlot::Initialize()
{
    if (Config->EnableCGroup) {
        try {
            ProcessGroup.EnsureExistance();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create process group %v",
                ~ProcessGroup.GetFullPath().Quote()) << ex;
        }

#ifdef _linux_
        try {
            NCGroup::RunKiller(ProcessGroup.GetFullPath());
        } catch (const std::exception& ex) {
            // ToDo(psushin): think about more complex logic of handling fs errors.
            LOG_FATAL(ex, "Slot user cleanup failed (ProcessGroup: %v)", ~ProcessGroup.GetFullPath().Quote());
        }
#endif
    }

    try {
        NFS::ForcePath(Path, 0755);
        SandboxPath = NFS::CombinePaths(Path, "sandbox");
        DoCleanSandbox();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create slot directory %v",
            ~Path.Quote()) << ex;
    }

    try {
        DoCleanProcessGroups();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to clean slot cgroups") << ex;
    }
}

void TSlot::Acquire()
{
    IsFree_ = false;
}

bool TSlot::IsFree() const
{
    return IsFree_;
}

int TSlot::GetUserId() const
{
    return UserId;
}

const NCGroup::TNonOwningCGroup& TSlot::GetProcessGroup() const
{
    if (Config->EnableCGroup) {
        return ProcessGroup;
    } else {
        return NullCGroup;
    }
}

std::vector<Stroka> TSlot::GetCGroupPaths() const
{
    std::vector<Stroka> result;

    if (Config->EnableCGroup) {
        auto subgroupName = GetSlotProcessGroup(SlotIndex);

        for (const auto& type : NCGroup::GetSupportedCGroups()) {
            NCGroup::TNonOwningCGroup group(type, subgroupName);
            result.push_back(group.GetFullPath());
        }
        result.push_back(ProcessGroup.GetFullPath());
    }

    return result;
}

void TSlot::DoCleanSandbox()
{
    try {
        if (NFS::Exists(SandboxPath)) {
            if (UserId == EmptyUserId) {
                NFS::RemoveRecursive(SandboxPath);
            } else {
                RunCleaner(SandboxPath);
            }
        }
        IsClean = true;
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Failed to clean sandbox directory %v",
            ~SandboxPath.Quote()) << ex;
        LOG_ERROR(wrappedError);
        THROW_ERROR wrappedError;
    }
}

void TSlot::DoCleanProcessGroups()
{
    try {
        for (const auto& path : GetCGroupPaths()) {
            NCGroup::RemoveAllSubcgroups(path);
        }
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Failed to clean slot subcgroups for slot %v",
            SlotIndex) << ex;
        LOG_ERROR(wrappedError);
        THROW_ERROR wrappedError;
    }
}

void TSlot::Clean()
{
    try {
        DoCleanProcessGroups();
        DoCleanSandbox();
    } catch (const std::exception& ex) {
        LOG_FATAL("%v", ex.what());
    }
}

void TSlot::Release()
{
    YCHECK(IsClean);
    IsFree_ = true;
}

void TSlot::InitSandbox()
{
    YCHECK(!IsFree_);

    try {
        NFS::ForcePath(SandboxPath, 0777);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to create sandbox directory %v", ~SandboxPath.Quote());
    }

    LOG_INFO("Created slot sandbox directory %v", ~SandboxPath.Quote());

    IsClean = false;
}

void TSlot::MakeLink(
    const Stroka& linkName,
    const Stroka& targetPath,
    bool isExecutable)
{
    {
        // Take exclusive lock in blocking fashion to ensure that no 
        // forked process is holding an open descriptor to the target file.
        TFile file(targetPath, RdOnly | CloseOnExec);
        file.Flock(LOCK_EX);
    }

    auto linkPath = NFS::CombinePaths(SandboxPath, linkName);
    NFS::MakeSymbolicLink(targetPath, linkPath);
    NFS::SetExecutableMode(linkPath, isExecutable);
}

void TSlot::MakeFile(const Stroka& fileName, std::function<void (TOutputStream*)> dataProducer, bool isExecutable)
{
    auto path = NFS::CombinePaths(SandboxPath, fileName);
    {
        // NB! Races are possible between file creation and call to flock.
        // Unfortunately in Linux we cannot make it atomically.
        TFile file(path, CreateAlways | CloseOnExec);
        file.Flock(LOCK_EX | LOCK_NB);
        TFileOutput fileOutput(file);
        dataProducer(&fileOutput);
        NFS::SetExecutableMode(path, isExecutable);
    }

    {
        // Take exclusive lock in blocking fashion to ensure that no 
        // forked process is holding an open descriptor.
        TFile file(path, RdOnly | CloseOnExec);
        file.Flock(LOCK_EX);
    }
}

const Stroka& TSlot::GetWorkingDirectory() const
{
    return Path;
}

IInvokerPtr TSlot::GetInvoker()
{
    return SlotThread->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
