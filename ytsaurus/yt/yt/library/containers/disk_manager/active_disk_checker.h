#pragma once

#include "public.h"
#include "config.h"
#include "disk_info_provider.h"

#include <yt/yt/server/lib/misc/reboot_manager.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TActiveDiskChecker
    : public TRefCounted
{
public:
    TActiveDiskChecker(
        TDiskInfoProviderPtr diskInfoProvider,
        TRebootManagerPtr rebootManager,
        IInvokerPtr invoker);

    void Start();

    void OnDynamicConfigChanged(const TActiveDiskCheckerDynamicConfigPtr& newConfig);

private:
    const NContainers::TDiskInfoProviderPtr DiskInfoProvider_;
    const TRebootManagerPtr RebootManager_;
    const IInvokerPtr Invoker_;

    TAtomicIntrusivePtr<TActiveDiskCheckerDynamicConfig> Config_;

    NConcurrency::TPeriodicExecutorPtr ActiveDisksCheckerExecutor_;

    std::optional<int> ActiveDiskCount_;

    void OnActiveDisksCheck();

};

DEFINE_REFCOUNTED_TYPE(TActiveDiskChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
