#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TPoolResources
    : public NYTree::TYsonSerializable
{
public:
    NScheduler::TJobResourcesConfigPtr StrongGuaranteeResources;
    NScheduler::TJobResourcesConfigPtr BurstGuaranteeResources;
    NScheduler::TJobResourcesConfigPtr ResourceFlow;
    std::optional<int> MaxOperationCount;
    std::optional<int> MaxRunningOperationCount;

    TPoolResources();

    bool IsNonNegative();
    TPoolResourcesPtr operator-();
};

DEFINE_REFCOUNTED_TYPE(TPoolResources)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
