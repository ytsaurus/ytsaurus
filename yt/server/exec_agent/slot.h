#pragma once

#include "public.h"

#include <yt/core/misc/fs.h>

#include <yt/core/actions/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/bus/public.h>

#include <yt/ytlib/cgroup/cgroup.h>

#include <yt/ytlib/formats/format.h>

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
        std::vector<Stroka> paths,
        const Stroka& nodeId,
        IInvokerPtr invoker,
        int slotIndex,
        TNullable<int> userId);

    void Initialize();

    bool IsFree() const;
    TNullable<int> GetUserId() const;
    const NCGroup::TNonOwningCGroup& GetProcessGroup() const;
    std::vector<Stroka> GetCGroupPaths() const;
    int GetPathIndex() const;

    NBus::TTcpBusServerConfigPtr GetRpcServerConfig() const;
    NBus::TTcpBusClientConfigPtr GetRpcClientConfig() const;

    void Acquire(int pathIndex);
    void InitSandbox();
    void Clean();
    void Release();

    IInvokerPtr GetInvoker();

    //! Creates a symbolic link #linkName for #targetPath in the sandbox.
    void MakeLink(
        ESandboxKind sandboxKind,
        const Stroka& targetPath,
        const Stroka& linkName,
        bool isExecutable) noexcept;

    const Stroka& GetWorkingDirectory() const;

private:
    std::atomic<bool> IsFree_ = {true};
    bool IsClean_ = true;
    int PathIndex_ = 0;

    const std::vector<Stroka> Paths_;
    const Stroka NodeId_;
    const int SlotIndex_;
    const TNullable<int> UserId_;
    const IInvokerPtr Invoker_;

    std::vector<TEnumIndexedVector<Stroka, ESandboxKind>> SandboxPaths_;

    NCGroup::TNonOwningCGroup ProcessGroup_;
    NCGroup::TNonOwningCGroup NullCGroup_;

    NLogging::TLogger Logger;
    TSlotManagerConfigPtr Config_;


    void DoCleanSandbox(int pathIndex);
    void DoCleanProcessGroups();
    void DoResetProcessGroup();

    void LogErrorAndExit(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
