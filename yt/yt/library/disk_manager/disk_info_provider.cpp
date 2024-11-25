#include "disk_info_provider.h"

#include "config.h"
#include "disk_manager_proxy.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NDiskManager  {

////////////////////////////////////////////////////////////////////////////////

class TDiskInfoProvider
    : public IDiskInfoProvider
{
public:
    TDiskInfoProvider(
        IDiskManagerProxyPtr diskManagerProxy,
        TDiskInfoProviderConfigPtr config)
        : DiskManagerProxy_(std::move(diskManagerProxy))
        , Config_(std::move(config))
    { }

    const std::vector<std::string>& GetConfigDiskIds() final
    {
        return Config_->DiskIds;
    }

    TFuture<std::vector<TDiskInfo>> GetYTDiskInfos() final
    {
        auto diskInfosFuture = DiskManagerProxy_->GetDiskInfos();
        auto ytDiskPathsFuture = DiskManagerProxy_->GetYTDiskDevicePaths();
        auto diskYTPrefix = Config_->YTDiskPrefix;

        // Merge two futures and filter disks placed in /yt.
        return diskInfosFuture.Apply(BIND([=] (const std::vector<TDiskInfo>& diskInfos) {
            return ytDiskPathsFuture.Apply(BIND([=] (const THashSet<std::string>& diskPaths) {
                std::vector<TDiskInfo> disks;

                for (const auto& diskInfo : diskInfos) {
                    auto isYtDisk = false;
                    for (const auto& path : diskPaths) {
                        if (path.starts_with(diskInfo.DevicePath)) {
                            isYtDisk = true;
                            break;
                        }
                    }

                    for (const auto& path : diskInfo.PartitionFsLabels) {
                        if (path.starts_with(diskYTPrefix)) {
                            isYtDisk = true;
                            break;
                        }
                    }

                    if (isYtDisk) {
                        disks.push_back(diskInfo);
                    }
                }

                return disks;
            }));
        }));
    }

    TFuture<void> UpdateDiskCache() final
    {
        return DiskManagerProxy_->UpdateDiskCache();
    }

    TFuture<void> RecoverDisk(const std::string& diskId) final
    {
        return DiskManagerProxy_->RecoverDiskById(diskId, ERecoverPolicy::RecoverAuto);
    }

    TFuture<void> FailDisk(
        const std::string& diskId,
        const std::string& reason) final
    {
        return DiskManagerProxy_->FailDiskById(diskId, reason);
    }

    TFuture<bool> GetHotSwapEnabled() final
    {
        return DiskManagerProxy_->GetHotSwapEnabled();
    }

private:
    const IDiskManagerProxyPtr DiskManagerProxy_;
    const TDiskInfoProviderConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

IDiskInfoProviderPtr CreateDiskInfoProvider(
    IDiskManagerProxyPtr diskManagerProxy,
    TDiskInfoProviderConfigPtr config)
{
    return New<TDiskInfoProvider>(
        std::move(diskManagerProxy),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
