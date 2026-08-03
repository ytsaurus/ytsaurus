#include <yt/yt/flow/examples/cpp/proto_parser/proto/log_record.pb.h>

#include <yt/yt/flow/library/cpp/parsers/proto.h>

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

struct TLogRecordMessage
    : public TYsonMessage
{
    std::string Level;
    std::string Text;
    i64 SeenAtLevel = 0;

    REGISTER_YSON_STRUCT(TLogRecordMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("level", &TThis::Level)
            .Default();
        registrar.Parameter("text", &TThis::Text)
            .Default();
        registrar.Parameter("seen_at_level", &TThis::SeenAtLevel)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TLogRecordMessage);

////////////////////////////////////////////////////////////////////////////////

struct TLevelCountsState
    : public NYTree::TYsonStruct
{
    THashMap<std::string, i64> RecordCounts;

    REGISTER_YSON_STRUCT(TLevelCountsState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("record_counts", &TThis::RecordCounts)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf LevelCountsStateName = "level_counts";

class TProtoLogParserComputation
    : public TProtoTransformOrderedSourceComputation<TLogRecordProto>
{
public:
    using TProtoTransformOrderedSourceComputation<TLogRecordProto>::TProtoTransformOrderedSourceComputation;

    void DoInit(IJobInitContextPtr initContext) final
    {
        initContext->InitClient(StateClient_, LevelCountsStateName);
    }

    void DoProcessProto(const TInputMessageConstPtr& inputMessage, TLogRecordProto&& proto, IOutputCollectorPtr output) final
    {
        auto state = StateClient_.GetState(inputMessage->Key);
        auto record = New<TLogRecordMessage>();
        record->Level = proto.level();
        record->Text = proto.text();
        record->SeenAtLevel = ++state->RecordCounts[proto.level()];
        output->AddMessage(ConvertToMessage(record));
    }

    void DoProcessUnparsed(const TInputMessageConstPtr& /*inputMessage*/, TError /*error*/, IOutputCollectorPtr /*output*/) final
    { }

private:
    TMutableStateKeyClient<TLevelCountsState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TProtoLogParserComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    NYT::NFlow::TSimpleSpecBuilder builder;
    builder.RegisterStream<NYT::NFlow::NExample::TLogRecordMessage>("records");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
