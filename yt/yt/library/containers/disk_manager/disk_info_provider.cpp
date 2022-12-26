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

TFuture<std::vector<TDiskInfo>> TDiskInfoProvider::GetFailedYtDisks()
{
    auto diskInfosFuture = DiskManagerProxy_->GetDisks();
    auto ytDiskNamesFuture = DiskManagerProxy_->GetYtDiskDeviceNames();

    // Merge two futures and filter disks with failed states and placed in /yt.
    return diskInfosFuture.Apply(BIND([=] (const std::vector<TDiskInfo>& diskInfos) {
        return ytDiskNamesFuture.Apply(BIND([=] (const THashSet<TString>& diskNames) {
            std::vector<TDiskInfo> failedDisks;

            for (const auto& diskInfo : diskInfos) {
                if (diskInfo.State == EDiskState::Failed &&
                    diskNames.contains(diskInfo.DeviceName))
                {
                    failedDisks.emplace_back(diskInfo);
                }
            }

            return failedDisks;
        }));
    }));
}

TFuture<std::vector<TErrorOr<void>>> TDiskInfoProvider::RecoverDisks(const THashSet<TString>& diskIds)
{
    std::vector<TFuture<void>> recoverDiskFutures;
    recoverDiskFutures.reserve(diskIds.size());

    for (const auto& diskId : diskIds) {
        recoverDiskFutures.emplace_back(DiskManagerProxy_->RecoverDiskById(diskId, ERecoverPolicy::RecoverAuto));
    }

    return AllSet(recoverDiskFutures);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
