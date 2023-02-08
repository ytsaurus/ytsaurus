#include "disk_info_provider.h"

#include <yt/yt/library/containers/disk_manager/disk_manager_proxy.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

TDiskInfoProvider::TDiskInfoProvider(IDiskManagerProxyPtr diskManagerProxy)
    : DiskManagerProxy_(std::move(diskManagerProxy))
{ }

TFuture<std::vector<TDiskInfo>> TDiskInfoProvider::GetYtDiskInfos()
{
    auto diskInfosFuture = DiskManagerProxy_->GetDisks();
    auto ytDiskPathsFuture = DiskManagerProxy_->GetYtDiskMountPaths();

    // Merge two futures and filter disks placed in /yt.
    return diskInfosFuture.Apply(BIND([=] (const std::vector<TDiskInfo>& diskInfos) {
        return ytDiskPathsFuture.Apply(BIND([=] (const THashSet<TString>& diskPaths) {
            std::vector<TDiskInfo> disks;

            for (const auto& diskInfo : diskInfos) {
                for (const auto& partitionFsLabel : diskInfo.PartitionFsLabels) {
                    if (diskPaths.contains(partitionFsLabel)) {
                        disks.push_back(diskInfo);
                        break;
                    }
                }
            }

            return disks;
        }));
    }));
}

TFuture<void> TDiskInfoProvider::RecoverDisk(const TString& diskId)
{
    return DiskManagerProxy_->RecoverDiskById(diskId, ERecoverPolicy::RecoverAuto);
}

TFuture<void> TDiskInfoProvider::FailDisk(
    const TString& diskId,
    const TString& reason)
{
    return DiskManagerProxy_->FailDiskById(diskId, reason);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
