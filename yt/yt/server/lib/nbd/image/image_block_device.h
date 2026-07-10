#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NImage {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateImageBlockDevice(
    std::string exportId,
    TImageBlockDeviceConfigPtr config,
    IImageReaderPtr reader,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NImage
