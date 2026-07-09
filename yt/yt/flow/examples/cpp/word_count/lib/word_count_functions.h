#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

struct TWordMessage
    : public TYsonMessage
{
    std::string Word;

    REGISTER_YSON_STRUCT(TWordMessage);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! User parameters of TTextReadFunction, read from the static ``function_parameters`` block
//! of the reader's spec.
struct TTextReaderParameters
    : public NYTree::TYsonStruct
{
    //! Words shorter than this are dropped.
    i64 MinWordLength = 0;

    REGISTER_YSON_STRUCT(TTextReaderParameters);

    static void Register(TRegistrar registrar);
};

//! Reads text messages from the input queue and emits one TWordMessage per word (at least
//! TTextReaderParameters::MinWordLength long) into the "words" stream. Registered with
//! YT_FLOW_DEFINE_PROCESS_FUNCTION and wired to a source computation in the spec.
class TTextReadFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    i64 MinWordLength_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Counts word occurrences. For each word key, reads the current count from the external
//! state table, increments it by one, and writes it back.
class TWordCountFunction
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
