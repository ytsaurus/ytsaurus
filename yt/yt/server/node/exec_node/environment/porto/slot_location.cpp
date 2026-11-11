#include "slot_location.h"

#include "volume_manager.h"

#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/slot_location.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/fs.h>

#include <util/system/user.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

void RemoveVolumesFromPortoPlace(
    TNonNullPtr<TSlotLocation> location,
    int slotIndex,
    TNonNullPtr<IPortoVolumeManager> volumeManager,
    const TSlotManagerDynamicConfig& config,
    const THashSet<std::string>& preservedVolumePaths,
    bool initializing)
{
    const auto& Logger = ExecNodeLogger();

    auto portoPlacePath = location->GetSandboxPath(slotIndex, ESandboxKind::PortoPlace);

    if (!NFS::Exists(portoPlacePath)) {
        YT_TLOG_DEBUG("Porto place directory does not exist, skipping volume cleanup")
            .With("PortoPlace", portoPlacePath);
        return;
    }

    auto timeout = config.RemoveVolumesFromPortoPlaceTimeout;

    YT_TLOG_DEBUG("Cleaning up volumes from porto place")
        .With("SlotIndex", slotIndex)
        .With("PortoPlace", portoPlacePath)
        .With("Timeout", timeout)
        .With("PreservedVolumePaths", preservedVolumePaths);

    auto removeVolumesResult = WaitFor(volumeManager->RemoveVolumes(portoPlacePath, timeout, preservedVolumePaths));
    if (!removeVolumesResult.IsOK()) {
        static constexpr auto Message = "Failed to remove volumes from porto place"_sb;
        YT_TLOG_ERROR(Message)
            .With("PortoPlace", portoPlacePath)
            .With("SlotIndex", slotIndex)
            .With(removeVolumesResult);
        auto error = TError(Message)
            .With("porto_place", portoPlacePath)
            .With("slot_index", slotIndex)
            .With(removeVolumesResult);
        if (!initializing) {
            // It would be nice to disable just this particular slot index, not the whole slot.
            location->Disable(error);
        }
        THROW_ERROR error;
    }

    YT_TLOG_DEBUG("Cleaned up volumes from porto place")
        .With("SlotIndex", slotIndex)
        .With("PortoPlace", portoPlacePath);
}

void RemoveLayersFromPortoPlace(
    TNonNullPtr<TSlotLocation> location,
    int slotIndex,
    TNonNullPtr<IPortoVolumeManager> volumeManager,
    const TSlotManagerDynamicConfig& config,
    bool initializing)
{
    const auto& Logger = ExecNodeLogger();

    auto portoPlacePath = location->GetSandboxPath(slotIndex, ESandboxKind::PortoPlace);

    if (!NFS::Exists(portoPlacePath)) {
        YT_TLOG_DEBUG("Porto place directory does not exist, skipping layer cleanup")
            .With("PortoPlace", portoPlacePath);
        return;
    }

    auto timeout = config.RemoveLayersFromPortoPlaceTimeout;

    YT_TLOG_DEBUG("Cleaning up layers from porto place")
        .With("SlotIndex", slotIndex)
        .With("PortoPlace", portoPlacePath)
        .With("Timeout", timeout);

    auto removeLayersResult = WaitFor(volumeManager->RemoveLayers(portoPlacePath, timeout));
    if (!removeLayersResult.IsOK()) {
        static constexpr auto Message = "Failed to remove layers from porto place"_sb;
        YT_TLOG_ERROR(Message)
            .With("PortoPlace", portoPlacePath)
            .With("SlotIndex", slotIndex)
            .With(removeLayersResult);
        auto error = TError(Message)
            .With("porto_place", portoPlacePath)
            .With("slot_index", slotIndex)
            .With(removeLayersResult);
        if (!initializing) {
            // It would be nice to disable just this particular slot index, not the whole slot.
            location->Disable(error);
        }
        THROW_ERROR error;
    }

    YT_TLOG_DEBUG("Cleaned up layers from porto place")
        .With("SlotIndex", slotIndex)
        .With("PortoPlace", portoPlacePath);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void CleanPortoSlotResources(
    TNonNullPtr<TSlotLocation> location,
    int slotIndex,
    TNonNullPtr<IPortoVolumeManager> volumeManager,
    const TSlotManagerDynamicConfig& config,
    const THashSet<std::string>& preservedVolumePaths,
    bool initializing)
{
    RemoveVolumesFromPortoPlace(location, slotIndex, volumeManager, config, preservedVolumePaths, initializing);
    RemoveLayersFromPortoPlace(location, slotIndex, volumeManager, config, initializing);
}

#ifdef _linux_

TFuture<void> CreatePortoRootVolumeDirectories(
    TNonNullPtr<TSlotLocation> location,
    const IVolumePtr& rootVolume,
    int userId)
{
    YT_VERIFY(rootVolume);

    return BIND([rootVolume, userId] {
        const auto& rootVolumeMountPath = rootVolume->GetPath();
        YT_VERIFY(NFS::Exists(rootVolumeMountPath));

        struct TDirectory
        {
            std::string Path;
            bool RemoveIfExists = false;
        };

        static constexpr int DirectoryPermissions = 0777;

        // NB: Paths are relative and ordered in the creation sequence.
        // Must create directory before its subdirectory.
        static const TDirectory Directories[] = {
            {
                .Path = "slot",
                .RemoveIfExists = false,
            },
            {
                .Path = Format("slot/%v", GetSandboxRelPath(ESandboxKind::User)),
                .RemoveIfExists = false,
            },
            {
                .Path = Format("slot/%v", GetSandboxRelPath(ESandboxKind::Tmp)),
                .RemoveIfExists = true,
            },
        };

        const auto& Logger = ExecNodeLogger();
        YT_TLOG_DEBUG("Creating slot directories in root volume")
            .With("RootPath", rootVolumeMountPath);

        int nodeUid = getuid();

        auto createRootDirectoryConfig = [&] {
            auto rootConfig = New<NTools::TRootDirectoryConfig>();
            rootConfig->SlotPath = rootVolumeMountPath;
            rootConfig->UserId = nodeUid;
            rootConfig->Permissions = DirectoryPermissions;

            for (const auto& [relativePath, removeIfExists] : Directories) {
                auto path = NFS::CombinePaths(rootVolumeMountPath, relativePath);

                auto directory = New<NTools::TDirectoryConfig>();
                directory->Path = path;
                directory->UserId = userId;
                directory->Permissions = DirectoryPermissions;
                directory->RemoveIfExists = removeIfExists;
                rootConfig->Directories.push_back(std::move(directory));
            }

            return rootConfig;
        };

        auto directoryBuilderConfig = New<NTools::TDirectoryBuilderConfig>();
        directoryBuilderConfig->NodeUid = nodeUid;
        directoryBuilderConfig->NeedRoot = true;
        directoryBuilderConfig->RootDirectoryConfigs.push_back(createRootDirectoryConfig());

        RunTool<NTools::TRootDirectoryBuilderTool>(directoryBuilderConfig);

        YT_TLOG_DEBUG("Created slot directories in root volume")
            .With("RootPath", rootVolumeMountPath);
    })
        .AsyncVia(location->GetToolInvoker())
        .Run();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
