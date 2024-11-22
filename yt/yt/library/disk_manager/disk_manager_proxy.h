#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

struct IDiskManagerProxy
    : public virtual TRefCounted
{
    virtual TFuture<THashSet<TString>> GetYTDiskDevicePaths() = 0;

    virtual TFuture<std::vector<TDiskInfo>> GetDisks() = 0;

    virtual TFuture<void> RecoverDiskById(const TString& diskId, ERecoverPolicy recoverPolicy) = 0;

    virtual TFuture<void> FailDiskById(const TString& diskId, const TString& reason) = 0;

    virtual TFuture<bool> GetHotSwapEnabledFuture() = 0;

    virtual TFuture<void> UpdateDiskCache() = 0;

    virtual void OnDynamicConfigChanged(const TDiskManagerProxyDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiskManagerProxy)

////////////////////////////////////////////////////////////////////////////////

IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
