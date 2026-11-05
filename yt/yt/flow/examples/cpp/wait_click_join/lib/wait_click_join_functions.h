#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! User parameters of TJoinFunction, read from the static `processing_function_parameters`
//! block of the spec.
struct TJoinParameters
    : public NYTree::TYsonStruct
{
    //! How long after a hit to keep waiting for its actions before emitting the join.
    TDuration WaitForActions;

    REGISTER_YSON_STRUCT(TJoinParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Per-key join state: the times collected from the action stream and the hit payload.
struct TJoinState
    : public NYTree::TYsonStruct
{
    std::optional<ui64> ShowTime;
    std::optional<ui64> ClickTime;
    std::optional<std::string> HitPayload;

    REGISTER_YSON_STRUCT(TJoinState);

    static void Register(TRegistrar registrar);
};

using TJoinStatePtr = NYT::TIntrusivePtr<TJoinState>;

////////////////////////////////////////////////////////////////////////////////

//! Group-by key: matches the spec's group_by_schema (hash, hit_id, hit_time).
struct TJoinKey
    : public NYTree::TYsonStruct
{
    ui64 Hash{};
    std::string HitId;
    ui64 HitTime{};

    REGISTER_YSON_STRUCT(TJoinKey);

    static void Register(TRegistrar registrar);
};

using TJoinKeyPtr = NYT::TIntrusivePtr<TJoinKey>;

////////////////////////////////////////////////////////////////////////////////

//! Input message of the "action" stream.
struct TActionMessage
    : public TYsonMessage
{
    std::string HitId;
    ui64 HitTime{};
    bool IsClick{};
    ui64 ActionTime{};

    REGISTER_YSON_STRUCT(TActionMessage);

    static void Register(TRegistrar registrar);
};

//! Input message of the "hit" stream.
struct THitMessage
    : public TYsonMessage
{
    std::string HitId;
    ui64 HitTime{};
    std::string HitPayload;

    REGISTER_YSON_STRUCT(THitMessage);

    static void Register(TRegistrar registrar);
};

//! Output message of the "joined_action" stream.
struct TJoinedActionMessage
    : public TYsonMessage
{
    std::string HitId;
    ui64 HitTime{};
    bool IsClick{};
    ui64 ShowTime{};
    ui64 ClickTime{};
    std::string HitPayload;

    REGISTER_YSON_STRUCT(TJoinedActionMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Joins the "hit" and "action" streams per key: collects the hit payload and the
//! show/click action times into per-key state, sets a timer at the end of the wait window,
//! and on the timer emits a TJoinedActionMessage (if a show and a payload were seen) and
//! clears the state.
class TJoinFunction
    : public IProcessFunction
{
public:
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
    TDuration WaitForActions_;
    TMutableStateKeyClient<TJoinState> JoinStateClient_;

    TSystemTimestamp GetWindowEnd(const TJoinKeyPtr& ysonKey) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
