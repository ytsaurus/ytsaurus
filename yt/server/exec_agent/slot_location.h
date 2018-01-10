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
        const TString& id,
        bool detachedTmpfsUmount);

    TFuture<void> CreateSandboxDirectories(int slotIndex);

    TFuture<void> MakeSandboxCopy(
        int slotIndex,
        ESandboxKind kind,
        const TString& sourcePath,
        const TString& destinationName,
        bool executable);

    TFuture<void> MakeSandboxLink(
        int slotIndex,
        ESandboxKind kind,
        const TString& targetPath,
        const TString& linkName,
        bool executable);

    TFuture<TString> MakeSandboxTmpfs(
        int slotIndex,
        ESandboxKind kind,
        i64 size,
        int userId,
        const TString& path,
        bool enable,
        IMounterPtr mounter);

    TFuture<void> SetQuota(
        int slotIndex,
        TNullable<i64> diskSpaceLimit,
        TNullable<i64> inodeLimit,
        int userId);

    TFuture<void> MakeConfig(int slotIndex, NYTree::INodePtr config);

    TFuture<void> CleanSandboxes(int slotIndex, IMounterPtr mounter);

    TString GetSlotPath(int slotIndex) const;

    void IncreaseSessionCount();
    void DecreaseSessionCount();

private:
    const TSlotLocationConfigPtr Config_;
    const NCellNode::TBootstrap* Bootstrap_;

    NConcurrency::TActionQueuePtr LocationQueue_;

    const bool DetachedTmpfsUmount_;

    bool HasRootPermissions_;

    THashSet<TString> TmpfsPaths_;

    TDiskHealthCheckerPtr HealthChecker_;

    void Disable(const TError& error);
    void ValidateEnabled() const;

    static void ValidateNotExists(const TString& path);

    bool IsInsideTmpfs(const TString& path) const;

    void EnsureNotInUse(const TString& path) const;

    void ForceSubdirectories(const TString& filePath, const TString& sandboxPath) const;

    TString GetSandboxPath(int slotIndex, ESandboxKind sandboxKind) const;
    TString GetConfigPath(int slotIndex) const;
};

DEFINE_REFCOUNTED_TYPE(TSlotLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
