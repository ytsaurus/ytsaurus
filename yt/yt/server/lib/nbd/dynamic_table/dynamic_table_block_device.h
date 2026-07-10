#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NDynamicTable {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateDynamicTableBlockDevice(
    std::string deviceId,
    TDynamicTableBlockDeviceConfigPtr deviceConfig,
    NApi::IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NDynamicTable
