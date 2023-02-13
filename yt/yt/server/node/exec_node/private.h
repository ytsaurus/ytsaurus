#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchedulerConnector)

////////////////////////////////////////////////////////////////////////////////

struct TControllerAgentDescriptor
{
    TString Address;
    NScheduler::TIncarnationId IncarnationId;

    bool operator==(const TControllerAgentDescriptor& other) const noexcept;
    bool operator!=(const TControllerAgentDescriptor& other) const noexcept;

    bool Empty() const noexcept;

    explicit operator bool() const noexcept;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJob)

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ExecNodeLogger("ExecNode");
inline const NProfiling::TProfiler ExecNodeProfiler("/exec_node");

constexpr int TmpfsRemoveAttemptCount = 5;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

template <>
struct THash<NYT::NExecNode::TControllerAgentDescriptor>
{
    size_t operator () (const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const;
};
