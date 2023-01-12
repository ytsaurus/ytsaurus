#include "disk_info_provider.h"

#include <yt/yt/library/containers/disk_manager/disk_manager_proxy.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

TDiskInfoProvider::TDiskInfoProvider(TDiskManagerProxyPtr diskManagerProxy)
    : DiskManagerProxy_(std::move(diskManagerProxy))
{ }

TFuture<std::vector<TDiskInfo>> TDiskInfoProvider::GetYtDiskInfos(EDiskState state)
{
    auto diskInfosFuture = DiskManagerProxy_->GetDisks();
    auto ytDiskPathsFuture = DiskManagerProxy_->GetYtDiskMountPaths();

    // Merge two futures and filter disks with failed states and placed in /yt.
    return diskInfosFuture.Apply(BIND([=] (const std::vector<TDiskInfo>& diskInfos) {
        return ytDiskPathsFuture.Apply(BIND([=] (const THashSet<TString>& diskPaths) {
            std::vector<TDiskInfo> failedDisks;

            for (const auto& diskInfo : diskInfos) {
                if (diskInfo.State == state) {
                    for (const auto& partitionFsLabel : diskInfo.PartitionFsLabels) {
                        if (diskPaths.contains(partitionFsLabel)) {
                            failedDisks.push_back(diskInfo);
                            break;
                        }
                    }
                }
            }

            return failedDisks;
        }));
    }));
}

TFuture<void> TDiskInfoProvider::RecoverDisk(const TString& diskId)
{
    return DiskManagerProxy_->RecoverDiskById(diskId, ERecoverPolicy::RecoverAuto);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
