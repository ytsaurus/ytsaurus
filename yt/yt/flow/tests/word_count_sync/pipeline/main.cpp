#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/shared_range.h>

#include <util/string/split.h>

#include <algorithm>

namespace NYT::NFlow::NWordCountSync {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TWordMessage
    : public TYsonMessage
{
    std::string Word;

    REGISTER_YSON_STRUCT(TWordMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("word", &TThis::Word)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);

////////////////////////////////////////////////////////////////////////////////

//! Parameters of TWordCountFunction.
struct TWordCountParameters
    : public NYTree::TYsonStruct
{
    //! Words shorter than this are not counted; they are recorded and written to the skipped table.
    i64 MinWordLength = 0;
    //! Dynamic table the skipped words and their lengths are written into during the sync phase.
    NYPath::TRichYPath SkippedWordsTablePath;

    REGISTER_YSON_STRUCT(TWordCountParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("min_word_length", &TThis::MinWordLength)
            .Default(0);
        registrar.Parameter("skipped_words_table_path", &TThis::SkippedWordsTablePath);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Parameters of TStopWordsResource.
struct TStopWordsParameters
    : public NYTree::TYsonStruct
{
    std::vector<std::string> StopWords;

    REGISTER_YSON_STRUCT(TStopWordsParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("stop_words", &TThis::StopWords)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A trivial worker-shared resource carrying the configured set of words to ignore
//! entirely. Exercises IRuntimeInitContext::GetStaticResource end to end: declared in
//! the spec's `resources`, listed in the counter computation's `required_resource_ids`
//! and fetched by the process function in Init.
class TStopWordsResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TStopWordsParameters);

    using TResourceBase::TResourceBase;

    bool IsStopWord(const std::string& word) const
    {
        const auto& words = GetParameters()->StopWords;
        return std::find(words.begin(), words.end(), word) != words.end();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Splits each input text message into words and emits one TWordMessage per word.
class TTextReadFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto text = GetColumnValue<std::string>(message, "text");
        for (const auto& word : StringSplitter(text).SplitBySet(" \t\n\r").SkipEmpty()) {
            auto wordMessage = New<TWordMessage>();
            wordMessage->Word = word;
            output->AddMessage(context->ConvertToMessage(wordMessage));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Counts word occurrences in external state. Words from the stop-words resource are
//! dropped entirely; of the rest, words shorter than the configured length are skipped:
//! each skipped word and its length is buffered and, in the end-of-epoch sync phase,
//! written into the skipped-words dynamic table within the same transaction.
class TWordCountFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        auto parameters = initContext->GetParameters<TWordCountParameters>();
        MinWordLength_ = parameters->MinWordLength;
        SkippedWordsTablePath_ = parameters->SkippedWordsTablePath;
        StopWords_ = initContext->GetStaticResource("StopWords")->As<TStopWordsResource>();
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto word = GetColumnValue<std::string>(message, "word");
        if (StopWords_->IsStopWord(word)) {
            return;
        }
        if (std::ssize(word) < MinWordLength_) {
            Skipped_.emplace_back(word, std::ssize(word));
            return;
        }

        auto state = StateClient_.GetState(message->Key);
        auto count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

    void Sync(const IRetryableTransactionPtr& transaction, const IRuntimeContextPtr& /*context*/) override
    {
        if (Skipped_.empty()) {
            return;
        }

        auto nameTable = New<TNameTable>();
        auto wordId = nameTable->RegisterNameOrThrow("word");
        auto lengthId = nameTable->RegisterNameOrThrow("length");

        auto buffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;
        rows.reserve(Skipped_.size());
        for (const auto& [word, length] : Skipped_) {
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedStringValue(word, wordId));
            builder.AddValue(MakeUnversionedInt64Value(length, lengthId));
            rows.push_back(buffer->CaptureRow(builder.GetRow()));
        }
        Skipped_.clear();

        // Enroll the write into the epoch's transaction, so the skipped words are committed
        // atomically with the counting state.
        transaction->Apply(BIND([
            path = SkippedWordsTablePath_.GetPath(),
            nameTable = std::move(nameTable),
            rows = MakeSharedRange(std::move(rows), std::move(buffer))
        ] (const NApi::ITransactionPtr& transaction) {
            transaction->WriteRows(path, nameTable, rows);
        }));
    }

private:
    i64 MinWordLength_ = 0;
    NYPath::TRichYPath SkippedWordsTablePath_;
    TIntrusivePtr<TStopWordsResource> StopWords_;
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
    std::vector<std::pair<std::string, i64>> Skipped_;
};

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_PROCESS_FUNCTION(TTextReadFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TWordCountFunction, TWordCountParameters);
YT_FLOW_DEFINE_RESOURCE(TStopWordsResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWordCountSync

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    NYT::NFlow::TSimpleSpecBuilder builder;
    builder.RegisterStream<NYT::NFlow::NWordCountSync::TWordMessage>("words");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
