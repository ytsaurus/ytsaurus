#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBlockDevice)
DECLARE_REFCOUNTED_STRUCT(INbdServer)

DECLARE_REFCOUNTED_CLASS(TIdsConfig)
DECLARE_REFCOUNTED_CLASS(TUdsConfig)
DECLARE_REFCOUNTED_CLASS(TNbdServerConfig)

DECLARE_REFCOUNTED_CLASS(TCypressFileBlockDeviceConfig)
DECLARE_REFCOUNTED_CLASS(TMemoryBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
