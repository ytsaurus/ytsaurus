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

    bool IsFree() const;
    int GetUserId() const;
    const NCGroup::TNonOwningCGroup& GetProcessGroup() const;
    std::vector<Stroka> GetCGroupPaths() const;

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
        bool isExecutable) noexcept;

    //! Writes data from producer to #fileName.
    void MakeFile(
        const Stroka& fileName, 
        std::function<void (TOutputStream*)> dataProducer, 
        bool isExecutable = false);

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

    void LogErrorAndExit(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
