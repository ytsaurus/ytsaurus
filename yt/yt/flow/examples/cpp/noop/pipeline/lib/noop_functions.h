#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! Reads messages from a source and does nothing with them. Registered with
//! YT_FLOW_DEFINE_PROCESS_FUNCTION and wired to a source computation in the spec.
class TNoopFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
