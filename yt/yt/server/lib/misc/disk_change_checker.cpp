#include "disk_change_checker.h"
#include "private.h"
#include "config.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/library/containers/disk_manager/public.h>
#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>

#include <util/random/random.h>

namespace NYT {

using namespace NConcurrency;
using namespace NContainers;
using namespace NProfiling;
using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDiskChangeChecker::TDiskChangeChecker(
    TDiskInfoProviderPtr diskInfoProvider,
    IInvokerPtr invoker,
    TLogger logger)
    : DiskInfoProvider_(std::move(diskInfoProvider))
    , Invoker_(std::move(invoker))
    , OrchidService_(CreateOrchidService())
    , Logger(std::move(logger))
    , CheckerExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TDiskChangeChecker::OnDiskChangeCheck, MakeWeak(this)),
        TDuration::Minutes(1)))
{ }

void TDiskChangeChecker::SetDiskIdsMismatched()
{
    return DiskIdsMismatched_.store(true);
}

void TDiskChangeChecker::BuildOrchid(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("disk_ids_mismatched").Value(DiskIdsMismatched_.load())
        .EndMap();
}

IYPathServicePtr TDiskChangeChecker::CreateOrchidService()
{
    return IYPathService::FromProducer(BIND(&TDiskChangeChecker::BuildOrchid, MakeStrong(this)))
        ->Via(Invoker_);
}

IYPathServicePtr TDiskChangeChecker::GetOrchidService()
{
    return OrchidService_;
}

void TDiskChangeChecker::OnDiskChangeCheck()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto hotSwapEnabled = WaitFor(DiskInfoProvider_->GetHotSwapEnabledFuture());

    // Fast path.
    if (!hotSwapEnabled.IsOK()) {
        YT_LOG_DEBUG(hotSwapEnabled, "Failed to get hotswap creds");
        return;
    }

    if (!hotSwapEnabled.Value()) {
        YT_LOG_DEBUG(hotSwapEnabled, "Hot swap disabled");
        return;
    }

    auto diskInfosOrError = WaitFor(DiskInfoProvider_->GetYTDiskInfos());

    // Fast path.
    if (!diskInfosOrError.IsOK()) {
        YT_LOG_ERROR(diskInfosOrError, "Failed to list disk infos");
        return;
    }

    CheckDiskChange(diskInfosOrError.Value());
}

void TDiskChangeChecker::UpdateOldDiskIds(THashSet<TString> oldDiskIds)
{
    OldDiskIds_ = oldDiskIds;
}

const THashSet<TString>& TDiskChangeChecker::GetOldDiskIds() const
{
    return OldDiskIds_;
}

void TDiskChangeChecker::Start()
{
    CheckerExecutor_->Start();
}

void TDiskChangeChecker::CheckDiskChange(const std::vector<TDiskInfo>& diskInfos)
{
    THashSet<TString> diskIds;
    THashSet<TString> aliveDiskIds;
    THashSet<TString> oldDiskIds = GetOldDiskIds();
    THashSet<TString> configDiskIds;

    for (const auto& diskInfo : diskInfos) {
        diskIds.insert(diskInfo.DiskId);

        if (diskInfo.State == NContainers::EDiskState::Ok) {
            aliveDiskIds.insert(diskInfo.DiskId);
        }
    }

    for (const auto& diskId : DiskInfoProvider_->GetConfigDiskIds()) {
        configDiskIds.insert(diskId);
    }

    auto checkDisks = [] (const THashSet<TString>& oldDisks, const THashSet<TString>& newDisks) {
        for (const auto& newDiskId : newDisks) {
            if (!oldDisks.contains(newDiskId)) {
                return false;
            }
        }

        return true;
    };

    if (!oldDiskIds.empty() && !configDiskIds.empty()) {
        if (!checkDisks(oldDiskIds, aliveDiskIds) ||
            !checkDisks(configDiskIds, diskIds) ||
            !checkDisks(diskIds, configDiskIds))
        {
            YT_LOG_WARNING("Set disk ids mismatched flag");
            SetDiskIdsMismatched();
        }
    }

    UpdateOldDiskIds(aliveDiskIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
