#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

// Passed to the TCluster object every load iteration.
class TClusterConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    int AntiaffinityConstraintsUniqueBucketLimit;
    bool BypassValidationErrors;

    TClusterConfig()
    {
        RegisterParameter("antiaffinity_constraints_unique_bucket_limit", AntiaffinityConstraintsUniqueBucketLimit)
            .GreaterThan(0)
            .Default(50);
        RegisterParameter("bypass_validation_errors", BypassValidationErrors)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
