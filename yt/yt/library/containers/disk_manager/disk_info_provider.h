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

    TFuture<std::vector<TDiskInfo>> GetFailedYtDisks();

private:
    const TDiskManagerProxyPtr DiskManagerProxy_;
};

DEFINE_REFCOUNTED_TYPE(TDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
