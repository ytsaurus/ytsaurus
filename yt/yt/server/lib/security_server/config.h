#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TUserAccessValidatorDynamicConfig
    : public virtual NYTree::TYsonStruct
{
    TAsyncExpiringCacheConfigPtr BanCache;

    REGISTER_YSON_STRUCT(TUserAccessValidatorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserAccessValidatorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
