#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBlockDevice)
DECLARE_REFCOUNTED_STRUCT(INbdServer)

DECLARE_REFCOUNTED_CLASS(TIdsConfig)
DECLARE_REFCOUNTED_CLASS(TUdsConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdTestOptions)
DECLARE_REFCOUNTED_CLASS(TNbdServerConfig)

DECLARE_REFCOUNTED_CLASS(TFileSystemBlockDeviceConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTableBlockDeviceConfig)
DECLARE_REFCOUNTED_CLASS(TMemoryBlockDeviceConfig)

DECLARE_REFCOUNTED_STRUCT(IImageReader)

DECLARE_REFCOUNTED_STRUCT(IRandomAccessFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
