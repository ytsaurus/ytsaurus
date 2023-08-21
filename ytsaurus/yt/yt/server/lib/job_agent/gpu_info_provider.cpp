#include "gpu_info_provider.h"

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TGpuInfoProviderMock
    : public IGpuInfoProvider
{
    virtual std::vector<TGpuInfo> GetGpuInfos(TDuration /*checkTimeout*/)
    {
        THROW_ERROR_EXCEPTION("GPU info provider library is not available under this build configuration");
    }
};

DEFINE_REFCOUNTED_TYPE(TGpuInfoProviderMock)

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IGpuInfoProviderPtr CreateGpuInfoProvider(const TGpuInfoSourceConfigPtr& /*gpuInfoSource*/)
{
    return New<TGpuInfoProviderMock>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
