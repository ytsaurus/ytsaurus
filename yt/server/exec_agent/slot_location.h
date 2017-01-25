#pragma once

#include "public.h"
#include "private.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/misc/disk_location.h>

#include <yt/core/misc/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TSlotLocation
    : public TDiskLocation
{
    DEFINE_BYVAL_RO_PROPERTY(int, SessionCount);

public:
    TSlotLocation(
        const TSlotLocationConfigPtr& config,
        const NCellNode::TBootstrap* bootstrap,
        const Stroka& id,
        bool detachedTmpfsUmount);

    TFuture<void> CreateSandboxDirectories(int slotIndex);

    TFuture<void> MakeSandboxCopy(
        int slotIndex,
        ESandboxKind kind,
        const Stroka& sourcePath,
        const Stroka& destinationName,
        bool executable);

    TFuture<void> MakeSandboxLink(
        int slotIndex,
        ESandboxKind kind,
        const Stroka& targetPath,
        const Stroka& linkName,
        bool executable);

    TFuture<Stroka> MakeSandboxTmpfs(
        int slotIndex,
        ESandboxKind kind,
        i64 size,
        int userId,
        const Stroka& path,
        bool enable);

    TFuture<void> MakeConfig(int slotIndex, NYTree::INodePtr config);

    TFuture<void> CleanSandboxes(int slotIndex);

    Stroka GetSlotPath(int slotIndex) const;

    void IncreaseSessionCount();
    void DecreaseSessionCount();

private:
    const TSlotLocationConfigPtr Config_;
    const NCellNode::TBootstrap* Bootstrap_;

    NConcurrency::TActionQueuePtr LocationQueue_;

    const bool DetachedTmpfsUmount_;

    bool HasRootPermissions_;

    yhash_set<Stroka> TmpfsPaths_;

    TDiskHealthCheckerPtr HealthChecker_;

    void Disable(const TError& error);
    void ValidateEnabled() const;

    void ValidateNotExists(const Stroka& path) const;
    bool IsInsideTmpfs(const Stroka& path) const;

    void EnsureNotInUse(const Stroka& path) const;

    Stroka GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const;
    Stroka GetConfigPath(int slotIndex) const;
};

DEFINE_REFCOUNTED_TYPE(TSlotLocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
