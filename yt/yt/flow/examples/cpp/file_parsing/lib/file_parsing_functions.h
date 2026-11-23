#pragma once

#include "messages.h"

#include <yt/yt/flow/library/cpp/common/async_processor.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

//! The column the input queue source adds with the write time of the row, in milliseconds.
inline const std::string WriteTimeColumn = "write_time_ms";

inline const TStreamId ParsedStreamId("parsed");
inline const TStreamId RetriesStreamId("retries");
inline const TStreamId FailedStreamId("failed");
inline const TStreamId ParseTimerStreamId("parse_timer");

inline const TResourceId ParseExecutorResourceId("ParseExecutor");

////////////////////////////////////////////////////////////////////////////////

//! Turns every input file into its first #TParseTask.
class TEnqueueFileFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TParseFunctionParameters
    : public NYTree::TYsonStruct
{
    //! A file rejected this many times is written to the failed files.
    i64 MaxAttempts = 0;

    REGISTER_YSON_STRUCT(TParseFunctionParameters);

    static void Register(TRegistrar registrar);
};

//! The request of a key, kept until its parse completes.
struct TParseState
    : public NYTree::TYsonStruct
{
    TParseTaskPtr Task;

    REGISTER_YSON_STRUCT(TParseState);

    static void Register(TRegistrar registrar);
};

//! Parses every request on the #TParseExecutorResource of the worker.
/*!
 *  A request sets a swift timer that adds the parse to an async processor. A parse rejected by a busy
 *  executor goes back to the parse queue with the next attempt, which has another key and may land in
 *  another partition and on another worker. The other outcomes go to the parsed or the failed files.
 *
 *  A request for a key with a parse in flight is absorbed. Since the key holds the attempt, a new request
 *  for a file that has moved to a later attempt or got its result is parsed again, and both give a result.
 */
class TParseFunction
    : public IProcessFunction
{
public:
    static constexpr TStringBuf ParseProcessorName = "parse";

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
    i64 MaxAttempts_ = 0;
    TMutableStateKeyClient<TParseState> StateClient_;
    TAsyncProcessorHandle<TParseTask, TParseResult> Parser_;

    void OnParsed(
        const TCompletedAsyncTask<TParseTask, TParseResult>& completedTask,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
