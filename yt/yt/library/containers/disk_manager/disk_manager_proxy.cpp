#include "disk_manager_proxy.h"

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TDiskManagerProxyMock
    : public IDiskManagerProxy
{
    virtual TFuture<THashSet<TString>> GetYtDiskDevicePaths()
    {
        return MakeFuture<THashSet<TString>>(TError("Disk manager library is not available under this build configuration"));
    }

    virtual TFuture<std::vector<TDiskInfo>> GetDisks()
    {
        return MakeFuture<std::vector<TDiskInfo>>(TError("Disk manager library is not available under this build configuration"));
    }

    virtual TFuture<void> RecoverDiskById(const TString& /*diskId*/, ERecoverPolicy /*recoverPolicy*/)
    {
        return MakeFuture(TError("Disk manager library is not available under this build configuration"));
    }

    virtual TFuture<void> FailDiskById(const TString& /*diskId*/, const TString& /*reason*/)
    {
        return MakeFuture(TError("Disk manager library is not available under this build configuration"));
    }

    virtual TFuture<bool> GetHotSwapEnabledFuture()
    {
        return MakeFuture<bool>(TError("Disk manager library is not available under this build configuration"));
    }

    virtual TFuture<void> UpdateDiskCache()
    {
        return MakeFuture(TError("Disk manager library is not available under this build configuration"));
    }

    virtual void OnDynamicConfigChanged(const TDiskManagerProxyDynamicConfigPtr& /*newConfig*/)
    {
        // Do nothing
    }
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxyMock)

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr /*config*/)
{
    // This implementation is used when disk_manager_proxy_impl.cpp is not linked.

    return New<TDiskManagerProxyMock>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
