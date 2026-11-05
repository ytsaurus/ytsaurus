#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

namespace NExample {

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

//! Swift map with batching enabled (`allow_batching_with_relaxed_guarantees`). Merges every per-key batch of input messages into
//! a single output message that carries all input `event_id`s as a comma-separated list.
class TBatcher
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcessKey(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        const auto& messages = input->GetMessages();
        if (messages.empty()) {
            return;
        }
        std::vector<i64> eventIds;
        eventIds.reserve(messages.size());
        for (const auto& message : messages) {
            eventIds.push_back(GetColumnValue<i64>(message, "event_id"));
        }
        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(MakeUnversionedStringValue(JoinSeq(",", eventIds)), "event_ids");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TBatcher);

////////////////////////////////////////////////////////////////////////////////

//! Transform with a single partition. Explodes batched `event_ids` and writes each into the sink queue.
class TWriter
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto eventIdsStr = GetColumnValue<std::string>(message, "event_ids");
        for (const auto& eventIdStr : StringSplitter(eventIdsStr).Split(',').SkipEmpty()) {
            i64 eventId = FromString<i64>(eventIdStr.Token());
            auto builder = MakeOutputMessageBuilder("sink_event");
            builder.Payload().SetValue(MakeUnversionedInt64Value(eventId), "event_id");
            output->AddMessage(builder.Finish());
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExample

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
