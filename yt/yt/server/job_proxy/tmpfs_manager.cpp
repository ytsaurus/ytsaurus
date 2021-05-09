#include "tmpfs_manager.h"

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/statistics.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NFS;

const static NLogging::TLogger Logger("TmpfsManager");

////////////////////////////////////////////////////////////////////////////////

TTmpfsManager::TTmpfsManager(TTmpfsManagerConfigPtr config)
    : Config_(std::move(config))
    , MaximumTmpfsSizes_(Config_->TmpfsPaths.size(), 0)
{
    for (const auto& path : Config_->TmpfsPaths) {
        TmpfsDeviceIds.insert(GetDeviceId(path));
    }

    YT_LOG_DEBUG("Tmpfs manager instantiated (TmpfsPaths: %v, TmpfsDeviceIds: %v)",
        Config_->TmpfsPaths,
        TmpfsDeviceIds);
}

void TTmpfsManager::DumpTmpfsStatistics(
    TStatistics* statistics,
    const TString& path) const
{
    auto tmpfsSizes = GetTmpfsSizes();

    auto guard = ReaderGuard(MaximumTmpfsSizesLock_);

    for (int index = 0; index < std::ssize(tmpfsSizes); ++index) {
        statistics->AddSample(Format("%v/tmpfs_volumes/%v/size", path, index), tmpfsSizes[index]);
        statistics->AddSample(Format("%v/tmpfs_volumes/%v/max_size", path, index), MaximumTmpfsSizes_[index]);
    }

    statistics->AddSample(Format("%v/tmpfs_size", path), std::accumulate(tmpfsSizes.begin(), tmpfsSizes.end(), 0ll));
    statistics->AddSample(Format("%v/max_tmpfs_size", path), MaximumTmpfsSize_);
}

i64 TTmpfsManager::GetTmpfsSize() const
{
    auto tmpfsSizes = GetTmpfsSizes();
    return std::accumulate(tmpfsSizes.begin(), tmpfsSizes.end(), 0ll);
}

bool TTmpfsManager::IsTmpfsDevice(int deviceId) const
{
    return TmpfsDeviceIds.contains(deviceId);
}

bool TTmpfsManager::HasTmpfsVolumes() const
{
    return !Config_->TmpfsPaths.empty();
}

std::vector<i64> TTmpfsManager::GetTmpfsSizes() const
{
    std::vector<i64> tmpfsSizes(Config_->TmpfsPaths.size(), 0);

    auto guard = WriterGuard(MaximumTmpfsSizesLock_);

    for (int index = 0; index < std::ssize(Config_->TmpfsPaths); ++index) {
        const auto& tmpfsPath = Config_->TmpfsPaths[index];
        auto& tmpfsSize = tmpfsSizes[index];

        try {
            auto diskSpaceStatistics = GetDiskSpaceStatistics(tmpfsPath);
            if (diskSpaceStatistics.TotalSpace < diskSpaceStatistics.AvailableSpace) {
                YT_LOG_WARNING("Disk total space is less that disk available space (TmpfsPath: %v, TotalSpace: %v, AvailableSpace: %v)",
                    tmpfsPath,
                    diskSpaceStatistics.TotalSpace,
                    diskSpaceStatistics.AvailableSpace);
            } else {
                tmpfsSize = diskSpaceStatistics.TotalSpace - diskSpaceStatistics.AvailableSpace;
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get tmpfs size (TmpfsPath: %v)",
                tmpfsPath);
        }

        MaximumTmpfsSizes_[index] = std::max(MaximumTmpfsSizes_[index], tmpfsSize);
    }

    MaximumTmpfsSize_ = std::max<i64>(MaximumTmpfsSize_, std::accumulate(tmpfsSizes.begin(), tmpfsSizes.end(), 0ll));

    return tmpfsSizes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
