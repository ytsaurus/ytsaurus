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

    //! Returns total tmpfs volumes size.
    i64 GetTmpfsSize() const;

    //! Returns |true| if |deviceId| is a device id of some tmpfs volume
    //! and |false| otherwise.
    bool IsTmpfsDevice(int deviceId) const;

    bool HasTmpfsVolumes() const;

private:
    const TTmpfsManagerConfigPtr Config_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MaximumTmpfsSizesLock_);
    mutable std::vector<i64> MaximumTmpfsSizes_;
    mutable i64 MaximumTmpfsSize_ = 0;

    THashSet<int> TmpfsDeviceIds;

    std::vector<i64> GetTmpfsSizes() const;
};

DEFINE_REFCOUNTED_TYPE(TTmpfsManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
