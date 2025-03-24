#pragma once

#include "block_device.h"
#include "config.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateChunkBlockDevice(
    TString exportId,
    TChunkBlockDeviceConfigPtr config,
    NConcurrency::IThroughputThrottlerPtr readThrottler,
    NConcurrency::IThroughputThrottlerPtr writeThrottler,
    IInvokerPtr invoker,
    NRpc::IChannelPtr channel,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
