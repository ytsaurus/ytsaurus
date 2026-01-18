#pragma once

#include "block_device.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDynamicTableBlockDevice(
    TString deviceId,
    TDynamicTableBlockDeviceConfigPtr deviceConfig,
    NApi::NNative::IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
