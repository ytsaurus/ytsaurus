#pragma once

#include "public.h"

#include <yt/server/lib/job_proxy/config.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TTmpfsManager
    : public TRefCounted
{
public:
    TTmpfsManager(TTmpfsManagerConfigPtr config);

    void DumpTmpfsStatistics(
        TStatistics* statistcs,
        const TString& path) const;

    //! Returns total tmpfs volumes size.
    ui64 GetTmpfsSize() const;

    //! Returns |true| if |deviceId| is a device id of some tmpfs volume
    //! and |false| otherwise.
    bool IsTmpfsDevice(int deviceId) const;

    bool HasTmpfsVolumes() const;

private:
    const TTmpfsManagerConfigPtr Config_;

    mutable NConcurrency::TReaderWriterSpinLock MaximumTmpfsSizesLock_;
    mutable std::vector<ui64> MaximumTmpfsSizes_;
    mutable ui64 MaximumTmpfsSize_ = 0;

    THashSet<int> TmpfsDeviceIds;

    std::vector<ui64> GetTmpfsSizes() const;
};

DEFINE_REFCOUNTED_TYPE(TTmpfsManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
