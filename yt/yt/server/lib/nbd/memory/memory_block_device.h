#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/library/nbd/memory/config.h>

namespace NYT::NNbd::NMemory {

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateMemoryBlockDevice(TMemoryBlockDeviceConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NMemory
