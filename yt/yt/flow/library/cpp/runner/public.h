#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/companion/public.h>
#include <yt/yt/flow/library/cpp/controller/public.h>
#include <yt/yt/flow/library/cpp/worker/public.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>

#include <util/generic/string.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline const std::string FlowModeEnvVarName = "YT_FLOW_MODE";

DECLARE_REFCOUNTED_STRUCT(IFlowNode)

DECLARE_REFCOUNTED_STRUCT(TFlowNodeConfig)

DEFINE_BIT_ENUM(EFlowRunMode,
    ((Worker)        (0x0001))
    ((Controller)    (0x0002))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
