#pragma once

#include "common.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

struct TWorkersDescription
    : public NYTree::TYsonStructLite
{
    std::vector<TWorkerDescription> Workers;

    REGISTER_YSON_STRUCT_LITE(TWorkersDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

TWorkersDescription DescribeWorkers(const TFlowViewPtr& flowView);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
