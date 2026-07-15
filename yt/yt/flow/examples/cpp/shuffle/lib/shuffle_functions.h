#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! Reads events from the input queue: parses the JSON in the "data" column and emits one
//! message into the "event" stream with the key_a..key_d and value fields. The "event"
//! stream schema is declared in the pipeline spec (there is no YSON message struct).
//! Registered with YT_FLOW_DEFINE_PROCESS_FUNCTION and wired to a source computation.
class TQueueReader
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

//! Counts events per group-by key. For each key, reads the current count from the external
//! "/state" table, increments it by one, and writes it back.
class TReducer
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
