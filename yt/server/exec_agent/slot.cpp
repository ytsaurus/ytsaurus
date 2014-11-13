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
    TNullable<int> userId)
    : IsFree_(true)
    , IsClean_(true)
    , Path_(path)
    , SlotIndex(slotIndex)
    , UserId_(userId)
    , SlotThread_(New<TActionQueue>(Format("ExecSlot:%v", slotIndex)))
    , ProcessGroup_("freezer", GetSlotProcessGroup(slotIndex))
    , NullCGroup_()
    , Logger(ExecAgentLogger)
    , Config_(config)
{
    Logger.AddTag("Slot: %v", SlotIndex);
}

void TSlot::Initialize()
{
    if (Config_->EnableCGroups) {
        try {
            ProcessGroup_.EnsureExistance();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create process group %v",
                ProcessGroup_.GetFullPath()) << ex;
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
    }

    try {
        NFS::ForcePath(Path_, 0755);
        SandboxPath_ = NFS::CombinePaths(Path_, "sandbox");
        DoCleanSandbox();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create slot directory %v",
            Path_) << ex;
    }

    try {
        DoCleanProcessGroups();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to clean slot cgroups")
            << ex;
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
        result.push_back(ProcessGroup_.GetFullPath());
    }

    return result;
}

void TSlot::DoCleanSandbox()
{
    try {
        if (NFS::Exists(SandboxPath_)) {
            if (UserId_.HasValue()) {
                RunCleaner(SandboxPath_);
            } else {
                NFS::RemoveRecursive(SandboxPath_);
            }
        }
        IsClean_ = true;
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Failed to clean sandbox directory %v",
            SandboxPath_)
            << ex;
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
    YCHECK(IsClean_);
    IsFree_ = true;
}

void TSlot::InitSandbox()
{
    YCHECK(!IsFree_);

    try {
        NFS::ForcePath(SandboxPath_, 0777);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to create sandbox directory %Qv", SandboxPath_);
    }

    LOG_INFO("Created slot sandbox directory %Qv", SandboxPath_);

    IsClean_ = false;
}

void TSlot::MakeLink(
    const Stroka& targetPath,
    const Stroka& linkName,
    bool executable)
{
    {
        // Take exclusive lock in blocking fashion to ensure that no 
        // forked process is holding an open descriptor to the target file.
        TFile file(targetPath, RdOnly | CloseOnExec);
        file.Flock(LOCK_EX);
    }

    auto linkPath = NFS::CombinePaths(SandboxPath_, linkName);
    NFS::MakeSymbolicLink(targetPath, linkPath);
    NFS::SetExecutableMode(linkPath, executable);
}

void TSlot::MakeFile(
    const Stroka& fileName,
    std::function<void (TOutputStream*)> dataProducer,
    bool executable)
{
    auto path = NFS::CombinePaths(SandboxPath_, fileName);
    {
        // NB! Races are possible between file creation and call to flock.
        // Unfortunately in Linux we cannot make it atomically.
        TFile file(path, CreateAlways | CloseOnExec);
        file.Flock(LOCK_EX | LOCK_NB);
        TFileOutput fileOutput(file);
        dataProducer(&fileOutput);
        NFS::SetExecutableMode(path, executable);
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
    return Path_;
}

IInvokerPtr TSlot::GetInvoker()
{
    return SlotThread_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
