#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd::NImage {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TImageBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TFileSystemBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(IImageReader)
DECLARE_REFCOUNTED_STRUCT(IRandomAccessFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NImage
