#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! Input message of the "event" stream: a raw user event keyed by |Key|.
struct TEventMessage
    : public TYsonMessage
{
    ui64 Key{};
    std::string Data;

    REGISTER_YSON_STRUCT(TEventMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Message of the "request" stream: an outstanding async request.
struct TRequestMessage
    : public TYsonMessage
{
    ui64 RequestId{};
    ui64 Key{};
    std::string Request;

    REGISTER_YSON_STRUCT(TRequestMessage);

    static void Register(TRegistrar registrar);
};

using TRequestMessagePtr = NYT::TIntrusivePtr<TRequestMessage>;

////////////////////////////////////////////////////////////////////////////////

//! Message of the "response" stream: the result of a completed request.
struct TResponseMessage
    : public TYsonMessage
{
    ui64 RequestId{};
    ui64 Key{};
    i64 Length{};

    REGISTER_YSON_STRUCT(TResponseMessage);

    static void Register(TRegistrar registrar);
};

using TResponseMessagePtr = NYT::TIntrusivePtr<TResponseMessage>;

////////////////////////////////////////////////////////////////////////////////

//! Per-key state of TRequestProcessor: the pending request and how many attempts failed so far.
struct TDelayedRequestState
    : public NYTree::TYsonStruct
{
    ui64 FailedAttempts{};
    TRequestMessagePtr Request;

    REGISTER_YSON_STRUCT(TDelayedRequestState);

    static void Register(TRegistrar registrar);
};

using TDelayedRequestStatePtr = NYT::TIntrusivePtr<TDelayedRequestState>;

////////////////////////////////////////////////////////////////////////////////

//! Retries an async request per key until it succeeds. On a new request it resets the per-key
//! state and tries once; on failure it bumps the failed-attempt counter and schedules a retry
//! timer |Delay| after the current processing timestamp; on success it emits a TResponseMessage
//! and clears the state.
class TRequestProcessor
    : public IProcessFunction
{
public:
    static constexpr ui64 MaxRetries = 3;
    static constexpr ui64 Delay = 5;

    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TDelayedRequestState> RequestStateClient_;

    TSystemTimestamp GetNextAttempt(const IRuntimeContextPtr& context) const;

    bool IsRequestSuccessful(ui64 requestId, i64 failedAttempts) const;

    void TryRequest(
        TStateAccessor<TDelayedRequestState>& state,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) const;
};

////////////////////////////////////////////////////////////////////////////////

//! Turns events into requests and accumulates response lengths per key. On an "event" message it
//! emits a TRequestMessage; on a "response" message it adds the response length to the per-key
//! total kept in the "/state" external state table.
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
