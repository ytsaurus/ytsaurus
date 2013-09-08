#include "stdafx.h"
#include "slot.h"
#include "private.h"

#include <ytlib/misc/proc.h>

#include <ytlib/ytree/yson_producer.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSlot::TSlot(const Stroka& path, int slotId, int userId)
    : IsFree_(true)
    , IsClean(true)
    , Path(path)
    , SlotId(slotId)
    , UserId(userId)
    , SlotThread(New<TActionQueue>(Sprintf("ExecSlot:%d", slotId)))
    , Logger(ExecAgentLogger)
{
    Logger.AddTag(Sprintf("SlotId: %d", SlotId));
}

void TSlot::Initialize()
{
    try {
        NFS::ForcePath(Path, 0755);
        SandboxPath = NFS::CombinePaths(Path, "sandbox");
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create slot directory %s",
            ~Path.Quote()) << ex;
    }

#ifdef _linux_
    try {
        if (UserId > 0) {
            // Kill all processes of this pseudo-user for sanity reasons.
            KilallByUid(UserId);
        }
    } catch (const std::exception& ex) {
        // ToDo(psushin): think about more complex logic of handling fs errors.
        LOG_FATAL(ex, "Slot user cleanup failed (UserId: %d)",
            UserId);
    }
#endif

    Clean();
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

void TSlot::Clean()
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
        LOG_FATAL(ex, "Failed to clean sandbox directory %s",
            ~SandboxPath.Quote());
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
        LOG_FATAL(ex, "Failed to create sandbox directory %s",
            ~SandboxPath.Quote());
    }

    LOG_INFO("Created slot sandbox directory %s",
        ~SandboxPath.Quote());

    IsClean = false;
}

void TSlot::MakeLink(
    const Stroka& linkName,
    const Stroka& targetPath,
    bool isExecutable)
{
    auto linkPath = NFS::CombinePaths(SandboxPath, linkName);
    NFS::MakeSymbolicLink(targetPath, linkPath);
    NFS::SetExecutableMode(linkPath, isExecutable);
}

void TSlot::MakeEmptyFile(const Stroka& fileName)
{
    TFile file(NFS::CombinePaths(SandboxPath, fileName), CreateAlways);
    file.Close();
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
