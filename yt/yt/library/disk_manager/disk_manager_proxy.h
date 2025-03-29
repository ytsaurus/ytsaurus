#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

struct IDiskManagerProxy
    : public virtual TRefCounted
{
    virtual TFuture<THashSet<std::string>> GetYTDiskDevicePaths() = 0;
    virtual TFuture<std::vector<TDiskInfo>> GetDiskInfos() = 0;

    virtual TFuture<void> RecoverDiskById(const std::string& diskId, ERecoverPolicy recoverPolicy) = 0;
    virtual TFuture<void> FailDiskById(const std::string& diskId, const std::string& reason) = 0;

    virtual TFuture<bool> GetHotSwapEnabled() = 0;

    virtual TFuture<void> UpdateDiskCache() = 0;

    virtual bool IsStub() const = 0;

    virtual void Reconfigure(const TDiskManagerProxyDynamicConfigPtr& dynamiconfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiskManagerProxy)

////////////////////////////////////////////////////////////////////////////////

IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
