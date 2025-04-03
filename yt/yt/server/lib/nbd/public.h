#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBlockDevice)
DECLARE_REFCOUNTED_STRUCT(INbdServer)
DECLARE_REFCOUNTED_STRUCT(IChunkHandler)

DECLARE_REFCOUNTED_STRUCT(TIdsConfig)
DECLARE_REFCOUNTED_STRUCT(TUdsConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdTestOptions)
DECLARE_REFCOUNTED_STRUCT(TNbdServerConfig)

DECLARE_REFCOUNTED_STRUCT(TChunkBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TFileSystemBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicTableBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TMemoryBlockDeviceConfig)

DECLARE_REFCOUNTED_STRUCT(IImageReader)

DECLARE_REFCOUNTED_STRUCT(IRandomAccessFileReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
