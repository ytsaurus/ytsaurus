#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NStateJoinerTest {

////////////////////////////////////////////////////////////////////////////////

//! Per-user accumulated total, kept as internal per-key state by TAccumulatorFunction and read
//! back by TJoinerFunction through a state joiner.
struct TUserTotalState
    : public NYTree::TYsonStruct
{
    i64 Total = 0;

    REGISTER_YSON_STRUCT(TUserTotalState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Sums each user's "Amount" into its own internal per-key state ("total") and forwards the
//! user id (with a constant bucket) into the "users" stream.
class TAccumulatorFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TUserTotalState> TotalClient_;
};

////////////////////////////////////////////////////////////////////////////////

//! Joins TAccumulatorFunction's internal per-user state (via the "/user_total" state joiner) and
//! emits the stored total for each incoming user into the "results" stream.
class TJoinerFunction
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TJoinedStateKeyClient<TUserTotalState> TotalJoiner_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStateJoinerTest
