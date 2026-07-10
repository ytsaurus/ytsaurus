#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

class TInnerScope
    : public NYT::NFlow::TYsonMessage
{
public:
    TString Data;

    REGISTER_YSON_STRUCT(TInnerScope);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("data", &TInnerScope::Data);
    }
};

DEFINE_REFCOUNTED_TYPE(TInnerScope);

////////////////////////////////////////////////////////////////////////////////

class TCompositeStruct
    : public NYT::NFlow::TYsonMessage
{
public:
    NYT::TIntrusivePtr<TInnerScope> Data;

    REGISTER_YSON_STRUCT(TCompositeStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("data", &TCompositeStruct::Data);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder("data");

        if (message.PayloadSchema->FindColumn("data")->GetWireType() == NYT::NTableClient::EValueType::String) {
            auto dataValue = GetColumnValue<std::optional<TStringBuf>>(message, "data");
            if (!dataValue) {
                return;
            }
            builder.Payload().Set(*dataValue, "data");
        } else {
            auto dataColumn = GetColumnValue<std::optional<NYT::NYson::TYsonString>>(message, "data");
            if (!dataColumn) {
                return;
            }

            // METHOD 1
            auto offer = NYT::NFlow::ConvertToYsonMessage<TCompositeStruct>(message);

            // METHOD 2
            auto dataMap = NYT::NYTree::ConvertToNode(*dataColumn)->AsMap();
            TString outputData = dataMap->GetChildValueOrThrow<TString>("data");

            YT_VERIFY(offer->Data->Data == outputData);

            builder.Payload().Set(outputData, "data");
        }

        builder.Payload().Set(message.EventTimestamp.Underlying(), "event_time");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
