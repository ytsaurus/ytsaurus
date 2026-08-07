#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/connectors/static_table/arrival_order_table_sink.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <fcntl.h>
#include <unistd.h>

namespace NTest {

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

class TArrivalOrderSourceComputation
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoInit(IJobInitContextPtr /*initContext*/) override
    {
        const auto& sourceKey = GetContext()->Partition->SourceKey;
        THROW_ERROR_EXCEPTION_UNLESS(sourceKey, "Arrival order sink test requires a source key");
        GetOrCreateSink(TSinkId("static"), sourceKey, GetDynamicSpec());
    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder();
        for (TStringBuf column : {"partition_id", "sequence", "data_weight"}) {
            builder.Payload().SetValue(GetColumn(message, column), column);
        }
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TArrivalOrderSourceComputation);

////////////////////////////////////////////////////////////////////////////////

struct TCrashArrivalOrderTableSinkParameters
    : public NStaticTableConnector::TArrivalOrderTableSinkParameters
{
    std::string CrashSentinelPath;

    REGISTER_YSON_STRUCT(TCrashArrivalOrderTableSinkParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("crash_sentinel_path", &TThis::CrashSentinelPath)
            .Default();
    }
};

class TCrashAfterExternalCommitSink
    : public NStaticTableConnector::TArrivalOrderTableSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCrashArrivalOrderTableSinkParameters);

    using TSinkController = NStaticTableConnector::TArrivalOrderTableSinkController;
    using TArrivalOrderTableSink::TArrivalOrderTableSink;

    void Distribute(
        const TOutputMessageConstPtr& message,
        TOnDistributedCallback onDistributed) override
    {
        const auto sentinelPath = GetParameters()->CrashSentinelPath;
        TArrivalOrderTableSink::Distribute(
            message,
            TOnDistributedCallback::FromCallback([
                sentinelPath,
                onDistributed = std::move(onDistributed)
            ] () mutable {
                if (!sentinelPath.empty()) {
                    int fd = ::open(sentinelPath.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
                    if (fd >= 0) {
                        Y_UNUSED(::close(fd));
                        ::_exit(0);
                    }
                }
                onDistributed();
            }));
    }
};

YT_FLOW_DEFINE_SINK(TCrashAfterExternalCommitSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
