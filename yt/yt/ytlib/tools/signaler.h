#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

struct TSignalerConfig
    : public NYTree::TYsonSerializable
{
    std::vector<int> Pids;
    TString SignalName;

    TSignalerConfig();
};

DEFINE_REFCOUNTED_TYPE(TSignalerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignalerTool
{
    void operator()(const TSignalerConfigPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
