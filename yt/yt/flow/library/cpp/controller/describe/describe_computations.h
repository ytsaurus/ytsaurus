#pragma once

#include "common.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

struct TComputationsDescription
    : public NYTree::TYsonStructLite
{
    std::vector<TComputationDescription> Computations;

    REGISTER_YSON_STRUCT_LITE(TComputationsDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

TComputationsDescription DescribeComputations(const TFlowViewPtr& flowView);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
