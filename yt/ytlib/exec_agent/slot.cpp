#include "stdafx.h"
#include "slot.h"
#include "private.h"

#include <ytlib/misc/fs.h>
#include <util/folder/dirut.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSlot::TSlot(const Stroka& path, const Stroka& name)
    : IsEmpty_(true)
    , Path(path)
    , SlotThread(New<TActionQueue>(name))
{
    NFS::ForcePath(Path);
    SandboxPath = NFS::CombinePaths(Path, "sandbox");
}

void TSlot::Acquire()
{
    IsEmpty_ = false;
    NFS::ForcePath(SandboxPath);
    LOG_TRACE("Slot created sandbox path: %s", ~SandboxPath);
}

bool TSlot::IsEmpty() const 
{
    return IsEmpty_;
}

void TSlot::Clean()
{
    RemoveDirWithContents(SandboxPath);
    IsEmpty_ = true;
}

void TSlot::MakeLink(
    const Stroka& linkName, 
    const Stroka& targetPath, 
    bool isExecutable)
{
    auto linkPath = NFS::CombinePaths(SandboxPath, linkName);
    NFS::MakeSymbolicLink(targetPath, linkPath);
    // ToDo: fix set executable.
    //NFS::SetExecutableMode(linkPath, isExecutable);
}

const Stroka& TSlot::GetWorkingDirectory() const
{
    return Path;
}

IInvoker::TPtr TSlot::GetInvoker()
{
    return SlotThread->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
