#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

class TFederatedClientConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Bundle name which liveness should be checked on the background.
    std::optional<TString> BundleName;

    //! How often cluster liveness should be checked on the background.
    TDuration CheckClustersHealthPeriod;

    //! Maximum number of retry attempts to make.
    int ClusterRetryAttempts;

    REGISTER_YSON_STRUCT(TFederatedClientConfig);

    static void Register(TRegistrar registrar);
};
DEFINE_REFCOUNTED_TYPE(TFederatedClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
