#pragma once

#include "block_device.h"
#include "config.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDynamicTableBlockDevice(
    std::string deviceId,
    TDynamicTableBlockDeviceConfigPtr deviceConfig,
    NApi::IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
