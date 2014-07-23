#pragma once

#include "public.h"

#include <core/misc/fs.h>

#include <core/concurrency/action_queue.h>

#include <core/logging/log.h>

#include <ytlib/cgroup/cgroup.h>

#include <ytlib/formats/format.h>

#include <util/stream/file.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public TRefCounted
{
public:
    TSlot(
        TSlotManagerConfigPtr config,
        const Stroka& path,
        int slotIndex,
        int userId);

    void Initialize();

    bool IsFree() const;
    int GetUserId() const;
    const NCGroup::TNonOwningCGroup& GetProcessGroup() const;
    std::vector<Stroka> GetCGroupPaths() const;

    void Acquire();
    void InitSandbox();
    void Clean();
    void Release();

    IInvokerPtr GetInvoker();

    //! Creates a symbolic link in the sandbox.
    void MakeLink(const Stroka& linkName, const Stroka& targetPath, bool isExecutable);

    //! Creates a file named #fileName in the sandbox, fills it with data obtained from #dataProducer.
    void MakeFile(const Stroka& fileName, std::function<void (TOutputStream*)> dataProducer);

    const Stroka& GetWorkingDirectory() const;

private:
    volatile bool IsFree_;
    bool IsClean;

    Stroka Path;
    int SlotIndex;
    int UserId;

    Stroka SandboxPath;

    NConcurrency::TActionQueuePtr SlotThread;

    NCGroup::TNonOwningCGroup ProcessGroup;
    NCGroup::TNonOwningCGroup NullCGroup;

    NLog::TLogger Logger;
    TSlotManagerConfigPtr Config;

    void DoCleanSandbox();
    void DoCleanProcessGroups();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
