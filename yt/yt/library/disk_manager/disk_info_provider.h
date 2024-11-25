#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NDiskManager   {

////////////////////////////////////////////////////////////////////////////////

struct IDiskInfoProvider
    : public TRefCounted
{
    virtual const std::vector<std::string>& GetConfigDiskIds() = 0;

    virtual TFuture<std::vector<TDiskInfo>> GetYTDiskInfos() = 0;

    virtual TFuture<void> UpdateDiskCache() = 0;

    virtual TFuture<void> RecoverDisk(const std::string& diskId) = 0;

    virtual TFuture<void> FailDisk(
        const std::string& diskId,
        const std::string& reason) = 0;

    virtual TFuture<bool> GetHotSwapEnabled() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiskInfoProvider)

////////////////////////////////////////////////////////////////////////////////

IDiskInfoProviderPtr CreateDiskInfoProvider(
    IDiskManagerProxyPtr diskManagerProxy,
    TDiskInfoProviderConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
