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

    TFuture<std::vector<TDiskInfo>> GetYtDiskInfos(EDiskState state);

    TFuture<std::vector<TErrorOr<void>>> RecoverDisks(const THashSet<TString>& diskIds);

private:
    const TDiskManagerProxyPtr DiskManagerProxy_;
};

DEFINE_REFCOUNTED_TYPE(TDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
