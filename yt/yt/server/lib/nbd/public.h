#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBlockDevice)
DECLARE_REFCOUNTED_STRUCT(INbdServer)

DECLARE_REFCOUNTED_STRUCT(TIdsConfig)
DECLARE_REFCOUNTED_STRUCT(TUdsConfig)
DECLARE_REFCOUNTED_STRUCT(TNbdTestOptions)
DECLARE_REFCOUNTED_STRUCT(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
