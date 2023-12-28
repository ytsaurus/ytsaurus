#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/containers/disk_manager/public.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TDiskChangeChecker
    : public TRefCounted
{
public:
    TDiskChangeChecker(
        NContainers::TDiskInfoProviderPtr diskInfoProvider,
        IInvokerPtr controlInvoker,
        NLogging::TLogger logger);

    void Start();

    NYTree::IYPathServicePtr GetOrchidService();

private:
    const NContainers::TDiskInfoProviderPtr DiskInfoProvider_;
    const IInvokerPtr Invoker_;
    const NYTree::IYPathServicePtr OrchidService_;

    const NLogging::TLogger Logger;

    std::atomic<bool> DiskIdsMismatched_;
    THashSet<TString> OldDiskIds_;

    NConcurrency::TPeriodicExecutorPtr CheckerExecutor_;

    void OnDiskChangeCheck();

    void CheckDiskChange(const std::vector<NContainers::TDiskInfo>& diskInfos);

    void SetDiskIdsMismatched();

    void UpdateOldDiskIds(THashSet<TString> oldDiskIds);

    const THashSet<TString>& GetOldDiskIds() const;

    NYTree::IYPathServicePtr CreateOrchidService();

    void BuildOrchid(NYson::IYsonConsumer* consumer);
};

DEFINE_REFCOUNTED_TYPE(TDiskChangeChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

