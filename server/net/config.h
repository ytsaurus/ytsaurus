#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

class TNetManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    int MaxNoncesGenerationAttempts;
    TString PodFqdnSuffix;

    TNetManagerConfig()
    {
        RegisterParameter("max_nonces_generation_attempts", MaxNoncesGenerationAttempts)
            .GreaterThan(0)
            .Default(10);
        RegisterParameter("pod_fqdn_suffix", PodFqdnSuffix)
            .Default("test.yp-c.yandex.net");
    }
};

DEFINE_REFCOUNTED_TYPE(TNetManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NServer
} // namespace NYP
