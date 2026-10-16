#include "shuffle_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <library/cpp/json/json_reader.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

// [BEGIN example_shuffle_queue_reader]
void TQueueReader::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto data = GetColumnValue<std::string>(message, "data");
    ::NJson::TJsonValue json;
    ::NJson::ReadJsonTree(data, &json, /*throwOnError*/ true);
    auto builder = context->MakeOutputMessageBuilder("event");
    builder.Payload().Set<std::string>(json["value"].GetStringSafe(), "value");
    builder.Payload().Set<ui64>(json["key_a"].GetUIntegerSafe(), "key_a");
    builder.Payload().Set<ui64>(json["key_b"].GetUIntegerSafe(), "key_b");
    builder.Payload().Set<ui64>(json["key_c"].GetUIntegerSafe(), "key_c");
    builder.Payload().Set<ui64>(json["key_d"].GetUIntegerSafe(), "key_d");
    output->AddMessage(builder.Finish());
}

// [END example_shuffle_queue_reader]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN example_shuffle_reducer]
void TReducer::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(StateClient_, "/state");
}

void TReducer::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& /*context*/)
{
    auto state = StateClient_.GetState(message->Key);
    i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);

    count += 1;

    TPayloadBuilder builder(state->Schema);
    builder.Set(count, "count");
    state->Payload = builder.Finish();
}

// [END example_shuffle_reducer]

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
