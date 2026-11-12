#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/string/format.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Emits a deterministic typed row per input message so a replay produces a
// byte-identical row. The id column is the input MessageId (globally unique and
// stable across replay), which lets the test assert the exactly-once / at-least-once
// row set on the ClickHouse side.
class TTypedReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    static inline TStreamId OutputStreamId = TStreamId("rows");

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto id = std::string(message.MessageId.Underlying());
        TStringBuf sourceId(id);
        sourceId.NextTok(':');
        auto seq = LexicographicallyParse<i64>(sourceId.NextTok(':'));
        auto flag = (seq & 1) != 0;
        auto category = flag ? "odd" : "even";
        // The code sharding key routes these fixture values to shards a and b, respectively.
        constexpr TStringBuf ShardCodes[] = {"0005", "0004"};
        auto code = ShardCodes[(seq / 2) % 2];

        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(id), "id");
        builder.Payload().SetValue(MakeUnversionedInt64Value(seq), "seq");
        builder.Payload().SetValue(MakeUnversionedBooleanValue(flag), "flag");
        builder.Payload().SetValue(MakeUnversionedStringValue(category), "category");
        builder.Payload().SetValue(MakeUnversionedStringValue(code), "code");
        constexpr TStringBuf CommonTokens[] = {"", R"(;"a")", R"(;"b";"a")", R"(;"c")"};
        // The fixture-<seq> token identifies each source row even when an INSERT pre-aggregates rows.
        auto advertiserTokens = Format(R"(["fixture-%v"%v])", seq, CommonTokens[(seq / 8) % 4]);
        builder.Payload().SetValue(MakeUnversionedCompositeValue(advertiserTokens), "advertiser_tokens");
        builder.Payload().Set<ui64>(1, "clicks");
        builder.Payload().Set<ui64>(2, "impressions");
        builder.Payload().Set<ui64>(3, "cost");
        builder.Payload().Set<ui64>(4, "bid");
        builder.Payload().Set<ui64>(5, "impressions_in_top");
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
