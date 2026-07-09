#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TReaderParameters
    : public virtual TSwiftOrderedSourceComputation::TParameters
{
    std::string FailKey;
    std::string FailComment;

    REGISTER_YSON_STRUCT(TReaderParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("fail_key", &TThis::FailKey)
            .Default();
        registrar.Parameter("fail_comment", &TThis::FailComment)
            .Default();
    }
};

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TReaderParameters);

    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    static inline TStreamId OutputStreamId = TStreamId("data");

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto key = GetColumnValue<TStringBuf>(message, "key");
        if (!GetParameters()->FailKey.empty() && key == GetParameters()->FailKey) {
            THROW_ERROR_EXCEPTION("Got fail key %v. Comment: %v", key, GetParameters()->FailComment);
        }

        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(key), "key");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

class TProcessor
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    { }
};

YT_FLOW_DEFINE_COMPUTATION(TProcessor);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
