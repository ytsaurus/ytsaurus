#include "stdafx.h"
#include "slot.h"
#include "private.h"

#include <ytlib/misc/fs.h>
#include <ytlib/ytree/yson_producer.h>

#include <util/folder/dirut.h>
#include <util/stream/file.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): think about more complex logic of handling fs errors.

TSlot::TSlot(const Stroka& path, int id, int userId)
    : IsFree_(true)
    , IsClean(true)
    , Path(path)
    , UserId(userId)
    , SlotThread(New<TActionQueue>(Sprintf("ExecSlot:%d", id)))
{
    try {
        NFS::ForcePath(Path);
        SandboxPath = NFS::CombinePaths(Path, "sandbox");
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to create slot directory: %s",
            ~Path.Quote());
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

void TSlot::Clean()
{
    try {
        if (isexist(~SandboxPath)) {
            RemoveDirWithContents(SandboxPath);
        }
        IsClean = true;
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to clean sandbox directory: %s",
            ~SandboxPath.Quote());
    }
}

void TSlot::Release()
{
    YASSERT(IsClean);
    IsFree_ = true;
}

void TSlot::InitSandbox()
{
    YASSERT(!IsFree_);
    try {
        NFS::ForcePath(SandboxPath);
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to create sandbox directory: %s",
            ~SandboxPath.Quote());
    }
    IsClean = false;
    LOG_TRACE("Slot created sandbox path: %s", ~SandboxPath);
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


void TSlot::MakeFile(
    const Stroka& fileName,
    NYTree::TYsonProducer producer,
    const NFormats::TFormat& format)
{
    TFileOutput fileOutput(NFS::CombinePaths(SandboxPath, fileName));

    producer.Run(
        CreateConsumerForFormat(
            format,
            NFormats::EDataType::Tabular,
            &fileOutput).Get());
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
