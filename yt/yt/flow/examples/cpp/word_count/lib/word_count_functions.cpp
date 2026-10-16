#include "word_count_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <util/string/split.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TWordMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("word", &TThis::Word)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTextReaderParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("min_word_length", &TThis::MinWordLength)
        .Default(0);
}

// [BEGIN text_reader]
void TTextReadFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    MinWordLength_ = initContext->GetParameters<TTextReaderParameters>()->MinWordLength;
}

void TTextReadFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto text = GetColumnValue<std::string>(message, "text");
    for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
        if (std::ssize(word) < MinWordLength_) {
            continue;
        }
        auto wordMessage = New<TWordMessage>();
        wordMessage->Word = word;
        output->AddMessage(context->ConvertToMessage(wordMessage));
    }
}

// [END text_reader]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN word_counter]
void TWordCountFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(StateClient_, "/state");
}

void TWordCountFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& /*context*/)
{
    auto state = StateClient_.GetState(message->Key);
    i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
    TPayloadBuilder builder(state->Schema);
    builder.Set(count + 1, "count");
    state->Payload = builder.Finish();
}

// [END word_counter]

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
