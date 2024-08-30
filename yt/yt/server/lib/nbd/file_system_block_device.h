#pragma once

#include "block_device.h"
#include "config.h"

#include <yt/yt/server/lib/nbd/image_reader.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateFileSystemBlockDevice(
    TString exportId,
    TFileSystemBlockDeviceConfigPtr config,
    IImageReaderPtr reader,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
