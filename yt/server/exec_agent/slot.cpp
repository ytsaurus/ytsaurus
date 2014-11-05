#include "stdafx.h"
#include "slot.h"
#include "private.h"

#include <ytlib/cgroup/cgroup.h>

#include <core/logging/log_manager.h>

#include <core/misc/proc.h>
#include <core/misc/string.h>

#include <core/ytree/yson_producer.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

Stroka GetSlotProcessGroup(int slotId)
{
    return "slot" + ToString(slotId);
}

////////////////////////////////////////////////////////////////////////////////

TSlot::TSlot(const Stroka& path, int slotId, int userId)
    : IsFree_(true)
    , IsClean(true)
    , Path(path)
    , SlotId(slotId)
    , UserId(userId)
    , SlotThread(New<TActionQueue>(Sprintf("ExecSlot:%d", slotId)))
    , ProcessGroup("freezer", GetSlotProcessGroup(slotId))
    , Logger(ExecAgentLogger)
{
    Logger.AddTag(Sprintf("SlotId: %d", SlotId));
}

void TSlot::Initialize()
{
    try {
        ProcessGroup.EnsureExistance();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create process group %s",
            ~ProcessGroup.GetFullPath().Quote()) << ex;
    }

#ifdef _linux_
    try {
        KillAll(BIND(&NCGroup::TNonOwningCGroup::GetTasks, &ProcessGroup));
    } catch (const std::exception& ex) {
        // ToDo(psushin): think about more complex logic of handling fs errors.
        LOG_FATAL(ex, "Slot user cleanup failed (ProcessGroup: %s)", ~ProcessGroup.GetFullPath().Quote());
    }
#endif

    try {
        NFS::ForcePath(Path, 0755);
        SandboxPath = NFS::CombinePaths(Path, "sandbox");
        DoCleanSandbox();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create slot directory %s",
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
    return ProcessGroup;
}

std::vector<Stroka> TSlot::GetCGroupPaths() const
{
    std::vector<Stroka> result;

    auto subgroupName = GetSlotProcessGroup(SlotId);

    for (const auto& type : NCGroup::GetSupportedCGroups()) {
        NCGroup::TNonOwningCGroup group(type, subgroupName);
        result.push_back(group.GetFullPath());
    }
    result.push_back(ProcessGroup.GetFullPath());

    return result;
}

void TSlot::DoCleanSandbox()
{
    try {
        if (isexist(~SandboxPath)) {
            if (UserId == EmptyUserId) {
                RemoveDirWithContents(SandboxPath);
            } else {
                RemoveDirAsRoot(SandboxPath);
            }
        }
        IsClean = true;
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Failed to clean sandbox directory %s",
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
        auto wrappedError = TError("Failed to clean slot subcgroups for slot %d",
            SlotId) << ex;
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
        LOG_FATAL("%s", ex.what());
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
        LOG_FATAL(ex, "Failed to create sandbox directory %s", ~SandboxPath.Quote());
    }

    LOG_INFO("Created slot sandbox directory %s", ~SandboxPath.Quote());

    IsClean = false;
}

void TSlot::MakeLink(
    const Stroka& linkName,
    const Stroka& targetPath,
    bool isExecutable) noexcept
{
    auto linkPath = NFS::CombinePaths(SandboxPath, linkName);
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
            "Failed to create a symlink in the slot %s (LinkPath: %s, TargetPath: %s, IsExecutable: %s)",
            ~SandboxPath.Quote(),
            ~linkPath.Quote(),
            ~targetPath.Quote(),
            ~FormatBool(isExecutable))
            << ex);
    }
}

void TSlot::LogErrorAndExit(const TError& error)
{
    LOG_ERROR(error);
    NLog::TLogManager::Get()->Shutdown();
    _exit(1);
}

void TSlot::MakeFile(const Stroka& fileName, std::function<void (TOutputStream*)> dataProducer, bool isExecutable)
{
    auto path = NFS::CombinePaths(SandboxPath, fileName);

    auto error = TError("Failed to create a file in the slot %s (FileName: %s, IsExecutable: %s)",
            ~Path.Quote(),
            ~fileName.Quote(),
            ~FormatBool(isExecutable));

    try {
        // NB! Races are possible between file creation and call to flock.
        // Unfortunately in Linux we cannot make it atomically.
        TFile file(path, CreateAlways | CloseOnExec);
        file.Flock(LOCK_EX | LOCK_NB);
        TFileOutput fileOutput(file);

        // Producer may throw non IO-related exceptions, that we do not handle.
        dataProducer(&fileOutput);
    } catch (const TFileError& ex) {
        LogErrorAndExit(error << TError(ex.what())  );
    }

    try {
        NFS::SetExecutableMode(path, isExecutable);

        // Take exclusive lock in blocking fashion to ensure that no
        // forked process is holding an open descriptor.
        TFile file(path, RdOnly | CloseOnExec);
        file.Flock(LOCK_EX);
    } catch (const std::exception& ex) {
        LogErrorAndExit(error << ex);
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
