#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TDiskInfoProvider
    : public TRefCounted
{
public:
    explicit TDiskInfoProvider(TDiskManagerProxyPtr diskManagerProxy);

    TFuture<std::vector<TDiskInfo>> GetYtDiskInfos();

    TFuture<void> RecoverDisk(const TString& diskId);

    TFuture<void> FailDisk(
        const TString& diskId,
        const TString& reason);

private:
    const TDiskManagerProxyPtr DiskManagerProxy_;
};

DEFINE_REFCOUNTED_TYPE(TDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
