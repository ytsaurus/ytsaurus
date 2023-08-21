#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/ytlib/scheduler/public.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TPoolResources
    : public NYTree::TYsonStruct
{
public:
    NScheduler::TJobResourcesConfigPtr StrongGuaranteeResources;
    NScheduler::TJobResourcesConfigPtr BurstGuaranteeResources;
    NScheduler::TJobResourcesConfigPtr ResourceFlow;
    std::optional<int> MaxOperationCount;
    std::optional<int> MaxRunningOperationCount;

    bool IsNonNegative();
    TPoolResourcesPtr operator-();

    REGISTER_YSON_STRUCT(TPoolResources);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolResources)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
