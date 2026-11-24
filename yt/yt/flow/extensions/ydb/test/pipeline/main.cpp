#include <yt/yt/flow/extensions/ydb/test/proto/message.pb.h>

#include "delivery_validation.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/parsers/proto.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/extensions/ydb/cpp/attributes.h>
#include <yt/yt/flow/extensions/ydb/cpp/source.h>
#include <yt/yt/flow/extensions/ydb/cpp/state.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

using namespace NYT::NFlow;
using namespace NYT;
using namespace NYT::NFlow::NYdbTest;

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TSourceReaderBase
    : public TBase
{
public:
    using TBase::TBase;

    void DoProcessProto(
        const TInputMessageConstPtr& inputMessage,
        NFlow::NProto::NTest::TTestMessage&& inputProto,
        IOutputCollectorPtr output) override
    {
        auto attributes = ForwardSourceAttributes(inputMessage).value_or(TYdbTopicAttributes{
            {"pipeline", "ydb-test"},
            {"key", inputProto.key()},
            {KafkaKeyAttributeName, inputProto.key()},
        });

        auto builder = this->MakeOutputMessageBuilder();
        builder.Payload().Set(inputProto.key(), "key");
        builder.Payload().Set(SerializeProtoToString(inputProto, false), "payload");
        builder.Payload().Set(NYson::TYsonStringBuf(PackYdbTopicAttributes(attributes)), "attributes");
        output->AddMessage(builder.Finish());
    }

    void DoProcessUnparsed(
        const TInputMessageConstPtr& inputMessage,
        TError /*error*/,
        IOutputCollectorPtr output) override
    {
        NFlow::NProto::NTest::TTestMessage outputProto;
        outputProto.set_key("unparsed");

        auto unparsed = NYTree::ConvertTo<TUnparsedYdbPayloadPtr>(
            GetColumnValue<NYson::TYsonStringBuf>(inputMessage, "unparsed"));
        outputProto.set_data(unparsed->Data->data(), unparsed->Data->size());

        TYdbTopicAttributes attributes{
            {"pipeline", "ydb-test"},
            {"key", outputProto.key()},
        };

        auto builder = this->MakeOutputMessageBuilder();
        builder.Payload().Set(outputProto.key(), "key");
        builder.Payload().Set(SerializeProtoToString(outputProto, false), "payload");
        builder.Payload().Set(NYson::TYsonStringBuf(PackYdbTopicAttributes(attributes)), "attributes");
        output->AddMessage(builder.Finish());
    }

    static std::optional<TYdbTopicAttributes> ForwardSourceAttributes(const TInputMessageConstPtr& inputMessage)
    {
        if (inputMessage->PayloadSchema->FindColumn("attributes") == nullptr) {
            return std::nullopt;
        }
        return UnpackYdbTopicAttributes(GetColumnValue<NYson::TYsonStringBuf>(inputMessage, "attributes"));
    }
};

class TSourceReader
    : public TSourceReaderBase<TProtoSwiftSourceComputation<NFlow::NProto::NTest::TTestMessage>>
{
public:
    using TSourceReaderBase::TSourceReaderBase;
};

YT_FLOW_DEFINE_COMPUTATION(TSourceReader);

////////////////////////////////////////////////////////////////////////////////

class TTransformSourceReader
    : public TSourceReaderBase<TProtoTransformOrderedSourceComputation<NFlow::NProto::NTest::TTestMessage>>
{
public:
    using TSourceReaderBase::TSourceReaderBase;
};

YT_FLOW_DEFINE_COMPUTATION(TTransformSourceReader);

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TExpectedProbeRange);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TDeliveryProbeGenerator, TDeliveryProbeGeneratorParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TDeliveryProbeOutputReader, TDeliveryProbeOutputReaderParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TDeliveryProbeExpectedReader, TDeliveryProbeGeneratorParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TDeliveryProbeValidator, TDeliveryProbeCombinedValidatorParameters);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
