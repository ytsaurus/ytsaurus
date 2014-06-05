#pragma once

#include "public.h"

#include <ytlib/cgroup/cgroup.h>

#include <core/concurrency/action_queue.h>

#include <core/formats/format.h>

#include <core/logging/tagged_logger.h>

#include <core/misc/fs.h>
#include <util/stream/file.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlot
    : public TRefCounted
{
public:
    TSlot(const Stroka& path, int slotId, int userId);

    void Initialize();

    int GetSlotId() const;
    bool IsFree() const;
    int GetUserId() const;
    const NCGroup::TNonOwningCGroup& GetProcessGroup() const;
    std::vector<Stroka> GetProcessGroups() const;

    void Acquire();
    void InitSandbox();
    void Clean();
    void Release();

    IInvokerPtr GetInvoker();

    //! Makes symbolic link on target inside slot sandbox.
    /*!
     *  Throws if operation fails.
     */
    void MakeLink(
        const Stroka& linkName,
        const Stroka& targetPath,
        bool isExecutable);

    //! Writes data from producer to #fileName.
    void MakeFile(const Stroka& fileName, std::function<void (TOutputStream*)> dataProducer);

    void MakeEmptyFile(const Stroka& fileName);

    const Stroka& GetWorkingDirectory() const;

private:
    volatile bool IsFree_;
    bool IsClean;

    Stroka Path;
    int SlotId;
    int UserId;

    Stroka SandboxPath;

    NConcurrency::TActionQueuePtr SlotThread;

    NCGroup::TNonOwningCGroup ProcessGroup;

    NLog::TTaggedLogger Logger;

    void DoCleanSandbox();
    void DoCleanProcessGroups();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
