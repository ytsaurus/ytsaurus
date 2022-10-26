#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TNativeDriverConfig
    : public TDriverConfig
{
public:
    NAuth::TTvmServiceConfigPtr TvmService;

    REGISTER_YSON_STRUCT(TNativeDriverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeDriverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
