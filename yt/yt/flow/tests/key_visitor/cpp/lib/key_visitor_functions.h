#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NKeyVisitorTest {

////////////////////////////////////////////////////////////////////////////////

//! Input messages keyed by `key` carry an arbitrary payload that the function stores in its
//! per-key state.
struct TKeyMessage
    : public TYsonMessage
{
    std::string Key;
    std::string Payload;

    REGISTER_YSON_STRUCT(TKeyMessage);

    static void Register(TRegistrar registrar);
};

//! Output produced on each visit: one row per key per pass, carrying the stored payload and a
//! monotonically increasing visit_index so the test can tell visits apart from re-emits.
struct TVisitMessage
    : public TYsonMessage
{
    std::string Key;
    std::string Payload;
    i64 VisitIndex = 0;

    REGISTER_YSON_STRUCT(TVisitMessage);

    static void Register(TRegistrar registrar);
};

//! Per-key user state of TVisitTesterFunction (internal-state variant).
struct TUserState
    : public NYTree::TYsonStruct
{
    std::string Payload;
    i64 VisitIndex = 0;

    REGISTER_YSON_STRUCT(TUserState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Stores each key's payload in internal per-key state on a message, and on a visit emits a
//! TVisitMessage with the stored payload and an incremented visit index.
class TVisitTesterFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

    void ProcessVisit(
        const TInputVisitConstPtr& visit,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TUserState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

//! Same behavior as TVisitTesterFunction, but the per-key state lives in a
//! TSimpleExternalStateManager-backed dynamic table instead of the per-job internal state.
class TExternalVisitTesterFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

    void ProcessVisit(
        const TInputVisitConstPtr& visit,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NKeyVisitorTest
