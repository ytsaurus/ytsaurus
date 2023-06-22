#include "active_disk_checker.h"

#include "private.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NContainers {

using namespace NConcurrency;
using namespace NContainers;
using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

TActiveDiskChecker::TActiveDiskChecker(
    TDiskInfoProviderPtr diskInfoProvider,
    TRebootManagerPtr rebootManager,
    IInvokerPtr invoker)
    : DiskInfoProvider_(std::move(diskInfoProvider))
    , RebootManager_(std::move(rebootManager))
    , Invoker_(std::move(invoker))
    , Config_(New<TActiveDiskCheckerDynamicConfig>())
{
    ActiveDisksCheckerExecutor_ = New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TActiveDiskChecker::OnActiveDisksCheck, MakeWeak(this)),
        Config_.Acquire()->CheckPeriod);
}

void TActiveDiskChecker::Start()
{
    if (Config_.Acquire()->Enabled) {
        YT_LOG_DEBUG("Starting active disks checker");
        ActiveDisksCheckerExecutor_->Start();
    }
}

void TActiveDiskChecker::OnDynamicConfigChanged(const TActiveDiskCheckerDynamicConfigPtr& newConfig)
{
    auto oldEnabled = Config_.Acquire()->Enabled;
    auto newEnabled = newConfig->Enabled;
    auto newCheckPeriod = newConfig->CheckPeriod;

    ActiveDisksCheckerExecutor_->SetPeriod(newCheckPeriod);

    if (oldEnabled && !newEnabled) {
        YT_LOG_DEBUG("Stopping active disk checker");
        YT_UNUSED_FUTURE(ActiveDisksCheckerExecutor_->Stop());
    } else if (!oldEnabled && newEnabled) {
        YT_LOG_DEBUG("Starting active disk checker");
        ActiveDisksCheckerExecutor_->Start();
    }

    Config_.Store(newConfig);
}

void TActiveDiskChecker::OnActiveDisksCheck()
{
    auto activeDisksOrError = WaitFor(DiskInfoProvider_->GetYtDiskInfos()
        .Apply(BIND([] (const std::vector<TDiskInfo>& diskInfos) {
            std::vector<TDiskInfo> activeDisks;

            for (const auto& disk : diskInfos) {
                if (disk.State == EDiskState::Ok) {
                    activeDisks.push_back(std::move(disk));
                }
            }

            return activeDisks;
        })));

    // Fast path.
    if (!activeDisksOrError.IsOK()) {
        YT_LOG_ERROR(activeDisksOrError, "Failed to get active disks");
        return;
    }

    auto currentCount = std::ssize(activeDisksOrError.Value());

    if (ActiveDiskCount_ && currentCount > ActiveDiskCount_) {
        // Start node reboot (with ytcfgen config regeneration).

        YT_LOG_WARNING("Request node reboot (PreviousDiskCount: %v, CurrentDiskCount: %v)", ActiveDiskCount_.value(), currentCount);

        RebootManager_->RequestReboot();
    }

    ActiveDiskCount_ = currentCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
