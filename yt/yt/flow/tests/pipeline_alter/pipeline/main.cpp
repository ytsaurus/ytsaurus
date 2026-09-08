#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Two yson messages with identical field sets registered under different names. Renaming the
// struct in user code and rebuilding the binary cannot be expressed inside one test binary, so
// both names are compiled in and |message_class_name| picks the one the "deployed" code builds.

struct TDataMessage
    : public TYsonMessage
{
    std::string Data;

    REGISTER_YSON_STRUCT(TDataMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("data", &TThis::Data)
            .Default();
    }
};

struct TRenamedDataMessage
    : public TYsonMessage
{
    std::string Data;

    REGISTER_YSON_STRUCT(TRenamedDataMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("data", &TThis::Data)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TDataMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TRenamedDataMessage);

////////////////////////////////////////////////////////////////////////////////

struct TReaderParameters
    : public TSwiftOrderedSourceComputation::TParameters
{
    //! Unset means the plain payload path; set makes the reader publish that yson message class,
    //! which the stream must then declare in its ``class_name``.
    std::optional<std::string> MessageClassName;

    REGISTER_YSON_STRUCT(TReaderParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("message_class_name", &TThis::MessageClassName)
            .Default();
    }
};

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TReaderParameters);

    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto className = GetParameters()->MessageClassName;
        if (!className) {
            auto builder = MakeOutputMessageBuilder("data");
            builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
            output->AddMessage(builder.Finish());
            return;
        }

        auto data = GetColumnValue<std::string>(message, "data");
        if (*className == TypeName<TRenamedDataMessage>()) {
            auto ysonMessage = New<TRenamedDataMessage>();
            ysonMessage->Data = std::move(data);
            output->AddMessage(ConvertToMessage(ysonMessage));
        } else {
            auto ysonMessage = New<TDataMessage>();
            ysonMessage->Data = std::move(data);
            output->AddMessage(ConvertToMessage(ysonMessage));
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
