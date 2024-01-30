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
    , MaxTmpfsUsage_(Config_->TmpfsPaths.size(), 0)
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
    auto tmpfsVolumesStatistics = GetTmpfsVolumeStatistics();

    auto guard = ReaderGuard(MaxTmpfsUsageLock_);

    i64 aggregatedTmpfsUsage = 0;
    i64 aggregatedTmpfsLimit = 0;

    for (int index = 0; index < std::ssize(tmpfsVolumesStatistics); ++index) {
        const auto& volumeStatistics = tmpfsVolumesStatistics[index];

        aggregatedTmpfsUsage += volumeStatistics.Usage;
        aggregatedTmpfsLimit += volumeStatistics.Limit;

        // COMPAT(ignat): size and max_size are misleading names.
        statistics->AddSample(Format("%v/tmpfs_volumes/%v/size", path, index), volumeStatistics.Usage);
        statistics->AddSample(Format("%v/tmpfs_volumes/%v/max_size", path, index), volumeStatistics.MaxUsage);

        statistics->AddSample(Format("%v/tmpfs_volumes/%v/usage", path, index), volumeStatistics.Usage);
        statistics->AddSample(Format("%v/tmpfs_volumes/%v/max_usage", path, index), volumeStatistics.MaxUsage);
        statistics->AddSample(Format("%v/tmpfs_volumes/%v/limit", path, index), volumeStatistics.Limit);
    }

    // COMPAT(ignat): tmpfs_size and max_tmpfs_size are misleading names.
    statistics->AddSample(Format("%v/tmpfs_size", path), aggregatedTmpfsUsage);
    statistics->AddSample(Format("%v/max_tmpfs_size", path), MaxAggregatedTmpfsUsage_);

    statistics->AddSample(Format("%v/tmpfs_usage", path), aggregatedTmpfsUsage);
    statistics->AddSample(Format("%v/tmpfs_max_usage", path), MaxAggregatedTmpfsUsage_);
    statistics->AddSample(Format("%v/tmpfs_limit", path), aggregatedTmpfsLimit);
}

i64 TTmpfsManager::GetAggregatedTmpfsUsage() const
{
    auto tmpfsVolumeStatistics = GetTmpfsVolumeStatistics();

    i64 aggregatedTmpfsUsage = 0;
    for (const auto& statistics : tmpfsVolumeStatistics) {
        aggregatedTmpfsUsage += statistics.Usage;
    }

    return aggregatedTmpfsUsage;
}

bool TTmpfsManager::IsTmpfsDevice(int deviceId) const
{
    return TmpfsDeviceIds.contains(deviceId);
}

bool TTmpfsManager::HasTmpfsVolumes() const
{
    return !Config_->TmpfsPaths.empty();
}

std::vector<TTmpfsManager::TTmpfsVolumeStatitsitcs> TTmpfsManager::GetTmpfsVolumeStatistics() const
{
    std::vector<TTmpfsVolumeStatitsitcs> tmpfsVolumeStatisitcs(Config_->TmpfsPaths.size());

    auto guard = WriterGuard(MaxTmpfsUsageLock_);

    i64 aggregatedTmpfsUsage = 0;

    for (int index = 0; index < std::ssize(Config_->TmpfsPaths); ++index) {
        const auto& tmpfsPath = Config_->TmpfsPaths[index];
        auto& volumeStatistics = tmpfsVolumeStatisitcs[index];

        try {
            auto diskSpaceStatistics = GetDiskSpaceStatistics(tmpfsPath);

            if (diskSpaceStatistics.TotalSpace < diskSpaceStatistics.AvailableSpace) {
                YT_LOG_WARNING("Disk total space is less that disk available space (TmpfsPath: %v, TotalSpace: %v, AvailableSpace: %v)",
                    tmpfsPath,
                    diskSpaceStatistics.TotalSpace,
                    diskSpaceStatistics.AvailableSpace);
            }

            volumeStatistics.Limit = diskSpaceStatistics.TotalSpace;
            volumeStatistics.Usage = std::max<i64>(0, diskSpaceStatistics.TotalSpace - diskSpaceStatistics.AvailableSpace);
            aggregatedTmpfsUsage += volumeStatistics.Usage;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get tmpfs disk space info (TmpfsPath: %v)",
                tmpfsPath);
        }

        volumeStatistics.MaxUsage = std::max(MaxTmpfsUsage_[index], volumeStatistics.Usage);
        MaxTmpfsUsage_[index] = volumeStatistics.MaxUsage;
    }

    MaxAggregatedTmpfsUsage_ = std::max<i64>(MaxAggregatedTmpfsUsage_, aggregatedTmpfsUsage);

    return tmpfsVolumeStatisitcs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
