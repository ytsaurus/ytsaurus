#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYP::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    // Require auth for all methods. See YP-1956
    bool RequireAuthenticationInSupplementaryCalls;


    TObjectServiceConfig()
    {
        RegisterParameter("require_authentication_in_supplementary_calls", RequireAuthenticationInSupplementaryCalls)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NApi
