#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/json/yson/json2yson.h>

#include <library/cpp/monlib/encode/json/json.h>

#include <util/stream/str.h>

namespace NYT::NFlow::NTables {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf CommitTimeSensor = "yt.flow.worker.computation.transaction_manager.commit_time.max";
constexpr TStringBuf SkippedEmptySensor = "yt.flow.worker.computation.transaction_manager.commit_skipped_empty.rate";

class TTransactionManagerMetricsTest
    : public ::testing::Test
{
protected:
    const TSolomonRegistryPtr Registry_ = New<TSolomonRegistry>();
    const TProfiler Profiler_{Registry_, "/flow/worker/computation"};
    const TIntrusivePtr<NiceMock<TMockClient>> Client_ = New<NiceMock<TMockClient>>();
    const TIntrusivePtr<NiceMock<TMockTransaction>> Transaction_ = New<NiceMock<TMockTransaction>>();
    TTransactionManagerPtr Manager_;

    void SetUp() override
    {
        Registry_->SetWindowSize(12);
        Transaction_->StartTimestamp = NTransactionClient::TTimestamp(1);

        ON_CALL(*Client_, StartTransaction(_, _))
            .WillByDefault(Return(MakeFuture<ITransactionPtr>(Transaction_)));
        ON_CALL(*Transaction_, Commit(_))
            .WillByDefault([] (const TTransactionCommitOptions& /*options*/) {
                return TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(5))
                    .Apply(BIND([] {
                        return TTransactionCommitResult{};
                    }));
            });

        auto context = New<TTransactionManagerContext>();
        context->Client = Client_;
        context->PipelinePath = "//pipeline";
        context->PartitionId = TPartitionId(TGuid::Create());
        context->LeaseId = TGuid::Create();
        context->Profiler = Profiler_;
        context->StatusProfiler = CreateSyncStatusProfiler();

        auto spec = New<TDynamicRetryableRequestSpec>();
        spec->LeaseCheckPeriod = TDuration::Hours(1);
        Manager_ = New<TTransactionManager>(context, spec);
    }

    THashMap<std::string, double> CollectMetrics()
    {
        Registry_->ProcessRegistrations();
        auto iteration = Registry_->GetNextIteration();
        Registry_->Collect();

        TReadOptions options;
        options.Times = {{{Registry_->IndexOf(iteration)}, TInstant::Zero()}};
        options.SummaryPolicy = ESummaryPolicy::Max;
        options.ConvertCountersToRateGauge = true;
        options.RateDenominator = 5.0;

        TStringStream buffer;
        auto encoder = ::NMonitoring::BufferedEncoderJson(&buffer);
        encoder->OnStreamBegin();
        Registry_->ReadSensors(options, encoder.Get());
        encoder->OnStreamEnd();
        encoder->Close();

        auto yson = NYson::TYsonString(
            NJson2Yson::SerializeJsonValueAsYson(NJson::ReadJsonFastTree(buffer.Str())));
        auto sensors = ConvertToNode(yson)->AsMap()->GetChildOrThrow("sensors")->AsList()->GetChildren();

        THashMap<std::string, double> result;
        for (const auto& sensor : sensors) {
            auto node = sensor->AsMap();
            auto name = node->GetChildOrThrow("labels")->AsMap()->GetChildValueOrThrow<std::string>("sensor");
            result.emplace(std::move(name), node->GetChildValueOrThrow<double>("value"));
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTransactionManagerMetricsTest, SuccessfulCommitRecordsDuration)
{
    EXPECT_CALL(*Transaction_, Commit(_)).Times(2);

    WaitFor(Manager_->CommitTransaction(Manager_->CreateTransaction()))
        .ThrowOnError();

    auto metrics = CollectMetrics();
    ASSERT_TRUE(metrics.contains(std::string(CommitTimeSensor)));
    EXPECT_GT(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.0);
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(SkippedEmptySensor)), 0.0);

    metrics = CollectMetrics();
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.0);
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(SkippedEmptySensor)), 0.0);
}

TEST_W(TTransactionManagerMetricsTest, EmptyCommitExportsFractionalRate)
{
    EXPECT_CALL(*Transaction_, Commit(_)).Times(2);

    WaitFor(Manager_->CommitTransaction(Manager_->CreateTransaction()))
        .ThrowOnError();
    CollectMetrics();

    WaitFor(Manager_->CommitTransaction(Manager_->CreateTransaction()))
        .ThrowOnError();

    auto metrics = CollectMetrics();
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.0);
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(SkippedEmptySensor)), 0.2);

    metrics = CollectMetrics();
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.0);
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(SkippedEmptySensor)), 0.0);
}

TEST_W(TTransactionManagerMetricsTest, CommitTimerExportsSubsecondDuration)
{
    auto timer = Profiler_.WithPrefix("/transaction_manager").Timer("/commit_time");
    timer.Record(TDuration::MilliSeconds(5));

    auto metrics = CollectMetrics();
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.005);
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(SkippedEmptySensor)), 0.0);

    metrics = CollectMetrics();
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.0);
}

TEST_W(TTransactionManagerMetricsTest, FailedCommitDoesNotReportProgress)
{
    EXPECT_CALL(*Transaction_, Commit(_))
        .WillOnce(Return(MakeFuture<TTransactionCommitResult>(TError(NYT::EErrorCode::Canceled, "Commit canceled"))));

    auto error = WaitFor(Manager_->CommitTransaction(Manager_->CreateTransaction()));
    EXPECT_FALSE(error.IsOK());

    auto metrics = CollectMetrics();
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(CommitTimeSensor)), 0.0);
    EXPECT_DOUBLE_EQ(GetOrCrash(metrics, std::string(SkippedEmptySensor)), 0.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NTables
