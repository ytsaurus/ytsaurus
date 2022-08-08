#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

struct TSignalerConfig
    : public NYTree::TYsonStruct
{
    std::vector<int> Pids;
    TString SignalName;

    REGISTER_YSON_STRUCT(TSignalerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSignalerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSignalerTool
{
    void operator()(const TSignalerConfigPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
