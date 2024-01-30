#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/config.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TTmpfsManager
    : public TRefCounted
{
public:
    explicit TTmpfsManager(TTmpfsManagerConfigPtr config);

    void DumpTmpfsStatistics(
        TStatistics* statistcs,
        const TString& path) const;

    //! Returns space used in tmpfs volumes.
    i64 GetAggregatedTmpfsUsage() const;

    //! Returns |true| if |deviceId| is a device id of some tmpfs volume
    //! and |false| otherwise.
    bool IsTmpfsDevice(int deviceId) const;

    bool HasTmpfsVolumes() const;

private:
    const TTmpfsManagerConfigPtr Config_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MaxTmpfsUsageLock_);
    mutable std::vector<i64> MaxTmpfsUsage_;
    mutable i64 MaxAggregatedTmpfsUsage_ = 0;

    THashSet<int> TmpfsDeviceIds;

    struct TTmpfsVolumeStatitsitcs
    {
        i64 Usage = 0;
        i64 MaxUsage = 0;
        i64 Limit = 0;
    };

    std::vector<TTmpfsVolumeStatitsitcs> GetTmpfsVolumeStatistics() const;
};

DEFINE_REFCOUNTED_TYPE(TTmpfsManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
