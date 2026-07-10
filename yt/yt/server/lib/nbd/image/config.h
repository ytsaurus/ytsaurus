#pragma once

#include "public.h"

#include <yt/yt/server/lib/nbd/config.h>

namespace NYT::NNbd::NImage {

////////////////////////////////////////////////////////////////////////////////

struct TImageBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    REGISTER_YSON_STRUCT(TImageBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TImageBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    REGISTER_YSON_STRUCT(TFileSystemBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileSystemBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NImage
