#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NNbd::NImage {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateFileSystemBlockDevice(
    std::string exportId,
    TFileSystemBlockDeviceConfigPtr config,
    IImageReaderPtr reader,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NImage
