#pragma once

#include "public.h"

#include <yt/yt/library/nbd/config.h>

namespace NYT::NNbd::NMemory {

////////////////////////////////////////////////////////////////////////////////

struct TMemoryBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    i64 Size;

    REGISTER_YSON_STRUCT(TMemoryBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NMemory
