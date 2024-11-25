#include "disk_manager_proxy.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

class TDiskManagerProxyStub
    : public IDiskManagerProxy
{
public:
    TFuture<THashSet<std::string>> GetYTDiskDevicePaths() final
    {
        return MakeFuture<THashSet<std::string>>(TError(NRpc::EErrorCode::NoSuchService, "Disk manager library is not available under this build configuration"));
    }

    TFuture<std::vector<TDiskInfo>> GetDiskInfos() final
    {
        return MakeFuture<std::vector<TDiskInfo>>(TError(NRpc::EErrorCode::NoSuchService, "Disk manager library is not available under this build configuration"));
    }

    TFuture<void> RecoverDiskById(const std::string& /*diskId*/, ERecoverPolicy /*recoverPolicy*/) final
    {
        return MakeFuture(TError(NRpc::EErrorCode::NoSuchService, "Disk manager library is not available under this build configuration"));
    }

    TFuture<void> FailDiskById(const std::string& /*diskId*/, const std::string& /*reason*/) final
    {
        return MakeFuture(TError(NRpc::EErrorCode::NoSuchService, "Disk manager library is not available under this build configuration"));
    }

    TFuture<bool> GetHotSwapEnabled() final
    {
        return MakeFuture<bool>(TError(NRpc::EErrorCode::NoSuchService, "Disk manager library is not available under this build configuration"));
    }

    TFuture<void> UpdateDiskCache() final
    {
        return MakeFuture(TError(NRpc::EErrorCode::NoSuchService, "Disk manager library is not available under this build configuration"));
    }

    void Reconfigure(const TDiskManagerProxyDynamicConfigPtr& /*newConfig*/) final
    {
        // Do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IDiskManagerProxyPtr CreateDiskManagerProxy(TDiskManagerProxyConfigPtr /*config*/)
{
    // This implementation is used when disk_manager_proxy_impl.cpp is not linked.
    return New<TDiskManagerProxyStub>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
