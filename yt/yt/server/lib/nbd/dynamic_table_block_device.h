#pragma once

#include "block_device.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDynamicTableBlockDevice(
    const TString& deviceId,
    TDynamicTableBlockDeviceConfigPtr deviceConfig,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
