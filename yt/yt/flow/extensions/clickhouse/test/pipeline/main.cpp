#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <util/digest/numeric.h>
#include <util/string/printf.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Emits a deterministic typed row per input message so a replay produces a
// byte-identical row. The id column is the input MessageId (globally unique and
// stable across replay), which lets the test assert the exactly-once / at-least-once
// / at-most-once row set on the ClickHouse side.
class TTypedReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    static inline TStreamId OutputStreamId = TStreamId("rows");

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto id = std::string(message.MessageId.Underlying());
        auto seq = static_cast<i64>(IntHash(THash<std::string>()(id)));
        auto flag = (seq & 1) != 0;
        auto category = flag ? "odd" : "even";
        auto code = Sprintf("%04d", static_cast<int>(seq & 0xFFF));

        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(id), "id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(seq), "seq");
        builder.Payload().SetValue(MakeUnversionedBooleanValue(flag), "flag");
        builder.Payload().SetValue(MakeUnversionedStringValue(category), "category");
        builder.Payload().SetValue(MakeUnversionedStringValue(code), "code");
        if (flag) {
            builder.Payload().SetValue(MakeUnversionedStringValue(id), "note");
        }
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTypedReader);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
