#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! Input message of the "event" stream.
struct TEventMessage
    : public TYsonMessage
{
    ui64 Key{};
    std::string Data;

    REGISTER_YSON_STRUCT(TEventMessage);

    static void Register(TRegistrar registrar);
};

//! Message of the "request" stream: a request to be processed downstream.
struct TRequestMessage
    : public TYsonMessage
{
    ui64 RequestId{};
    ui64 Key{};
    std::string Request;

    REGISTER_YSON_STRUCT(TRequestMessage);

    static void Register(TRegistrar registrar);
};

//! Message of the "response" stream: the length of a processed request.
struct TResponseMessage
    : public TYsonMessage
{
    ui64 RequestId{};
    ui64 Key{};
    i64 Length{};

    REGISTER_YSON_STRUCT(TResponseMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Reads a TRequestMessage and emits a TResponseMessage carrying the length of the request.
//! Hosted by TProcessFunctionSwiftMapComputation via the spec.
class TRequestProcessor
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

//! On the "event" stream emits a TRequestMessage with a random RequestId; on the "response"
//! stream accumulates the total response length into the external state "/state".
//! Hosted by TProcessFunctionComputation via the spec.
class TStateKeeper
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
