#pragma once

#include "block_device.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    const TString& exportId,
    TCypressFileBlockDeviceConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
