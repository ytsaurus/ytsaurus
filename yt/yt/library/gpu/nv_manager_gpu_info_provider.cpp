#include "nv_manager_gpu_info_provider.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IGpuInfoProviderPtr CreateNvManagerGpuInfoProvider(TGpuInfoSourceConfigPtr /*config*/)
{
    THROW_ERROR_EXCEPTION("NvManager GPU info provider is not supported in this build");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
