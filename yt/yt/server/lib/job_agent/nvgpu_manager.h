#include "gpu_info_provider.h"

#include <yt/yt/core/rpc/client.h>

#include <infra/rsm/nvgpumanager/api/nvgpu.pb.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using TReqListDevices = nvgpu::Empty;
using TRspListDevices = nvgpu::ListResponse;

class TNvGpuManagerService
    : public NYT::NRpc::TProxyBase
{
public:
    TNvGpuManagerService(NYT::NRpc::IChannelPtr channel, TString serviceName);

    DEFINE_RPC_PROXY_METHOD(NJobAgent, ListDevices);
};

void FromProto(TGpuInfo* gpuInfo, int index, const nvgpu::GpuDevice& device);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
