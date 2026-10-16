#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

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

#include <optional>

namespace NYT::NFlow::NEpochSeqNoSync {

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

//! Parameters of TSeqNoProbeFunction.
struct TSeqNoProbeParameters
    : public NYTree::TYsonStruct
{
    //! Dynamic table each epoch's unique seqno is written into during the sync phase.
    NYPath::TRichYPath SeqNosTablePath;

    REGISTER_YSON_STRUCT(TSeqNoProbeParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("seq_nos_table_path", &TThis::SeqNosTablePath);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Splits each input text message into words, and in the end-of-epoch sync phase reads
//! IRuntimeContext::GetEpochUniqueSeqNo. The sync phase runs on every epoch of the hosting
//! transform ordered-source computation, including epochs whose batch is empty, so the probe
//! exercises exactly the path where a stale (or absent) seqno would leak into user code: it
//! throws unless every observed value strictly exceeds the previous one, and records each
//! value into a dynamic table within the epoch's transaction for the test to assert on.
class TSeqNoProbeFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        SeqNosTablePath_ = initContext->GetParameters<TSeqNoProbeParameters>()->SeqNosTablePath;
    }

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

    void Sync(const IRetryableTransactionPtr& transaction, const IRuntimeContextPtr& context) override
    {
        auto seqNo = context->GetEpochUniqueSeqNo();
        THROW_ERROR_EXCEPTION_UNLESS(
            !LastSeqNo_ || seqNo > *LastSeqNo_,
            "Epoch unique seqno is not fresh: got %v after %v",
            seqNo.Underlying(),
            LastSeqNo_->Underlying());
        LastSeqNo_ = seqNo;

        auto nameTable = New<TNameTable>();
        auto seqNoId = nameTable->RegisterNameOrThrow("seq_no");
        auto okId = nameTable->RegisterNameOrThrow("ok");

        auto buffer = New<TRowBuffer>();
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(seqNo.Underlying(), seqNoId));
        builder.AddValue(MakeUnversionedBooleanValue(true, okId));
        std::vector<TUnversionedRow> rows{buffer->CaptureRow(builder.GetRow())};

        transaction->Apply(BIND([
            path = SeqNosTablePath_.GetPath(),
            nameTable = std::move(nameTable),
            rows = MakeSharedRange(std::move(rows), std::move(buffer))
        ] (const NApi::ITransactionPtr& transaction) {
            transaction->WriteRows(path, nameTable, rows);
        }));
    }

private:
    NYPath::TRichYPath SeqNosTablePath_;
    std::optional<TUniqueSeqNo> LastSeqNo_;
};

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_PROCESS_FUNCTION(TSeqNoProbeFunction, TSeqNoProbeParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NEpochSeqNoSync

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    NYT::NFlow::TSimpleSpecBuilder builder;
    builder.RegisterStream<NYT::NFlow::NEpochSeqNoSync::TWordMessage>("words");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
