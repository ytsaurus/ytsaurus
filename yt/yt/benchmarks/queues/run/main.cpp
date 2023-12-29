#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/queue_client/partition_reader.h>
#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/config.h>

namespace NYT {

using namespace NApi;
using namespace NAuth;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NProfiling;
using namespace NQueueClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NLogging;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const TLogger QueueBenchmarkLogger("QueueBenchmark");
static const auto& Logger = QueueBenchmarkLogger;

////////////////////////////////////////////////////////////////////////////////


DECLARE_REFCOUNTED_CLASS(TConfig)

class TConfig
    : public TSingletonsConfig
{
public:
    NRpcProxy::TConnectionConfigPtr Connection;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("connection", &TThis::Connection)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionReaderBenchmarkProgram final
    : public TProgram
{
public:
    TPartitionReaderBenchmarkProgram()
    {
        Opts_.AddLongOption("config").StoreResult(&ConfigFileName_).Required();
        Opts_.AddLongOption("consumer-path").StoreResult(&ConsumerPathRaw_).Required();
        Opts_.AddLongOption("queue-path").StoreResult(&QueuePathRaw_).Required();
        Opts_.AddLongOption("cluster").StoreResult(&Cluster_).Required();
        Opts_.AddLongOption("max-data-weight").StoreResult(&MaxDataWeight_).DefaultValue(16_MB);
        Opts_.AddLongOption("batch-logging-period").StoreResult(&BatchLoggingPeriod_).DefaultValue(10);
        Opts_.AddLongOption("tablet-cell-bundle").StoreResult(&TabletCellBundle_).DefaultValue("default");
        Opts_.AddLongOption("mode").StoreResult(&Mode_).DefaultValue("offline");
        Opts_.AddLongOption("use-native-tablet-node-api").StoreTrue(&UseNativeTabletNodeApi_);
        Opts_.AddLongOption("use-pull-consumer").StoreTrue(&UsePullConsumer_);
        Opts_.AddLongOption("use-yt-consumers").StoreTrue(&UseYTConsumers_);
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        Init();

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        // This limit is intentionally very big so that we use MaxDataWeight when reading.
        partitionReaderConfig->MaxRowCount = 1'000'000;
        partitionReaderConfig->MaxDataWeight = MaxDataWeight_;
        partitionReaderConfig->UseNativeTabletNodeApi = UseNativeTabletNodeApi_;
        partitionReaderConfig->UsePullConsumer = UsePullConsumer_;
        YT_LOG_INFO(
            "Partition reader config (UseNativeTabletNodeApi: %v, UsePullConsumer: %v)",
            partitionReaderConfig->UseNativeTabletNodeApi,
            partitionReaderConfig->UsePullConsumer);
        auto partitionReader = (UseYTConsumers_
            ? CreateMultiQueueConsumerPartitionReader(
            partitionReaderConfig,
            Client_,
            ConsumerPath_,
            QueuePath_,
            /*partition_index*/ 0)
            : CreatePartitionReader(
                partitionReaderConfig,
                Client_,
                ConsumerPath_.GetPath(),
                /*partition_index*/ 0));

        WaitFor(partitionReader->Open())
            .ThrowOnError();

        TStatisticsCollector statisticsCollector(BatchLoggingPeriod_);
        while (true) {
            auto traceContext = TTraceContext::NewRoot("QueueBenchmarkReadIteration");
            auto guard = TCurrentTraceContextGuard(traceContext);

            TWallTimer totalTimer;
            TFiberWallTimer totalCpuTimer;

            // Reading rowset.
            TWallTimer readTimer;
            auto rowsetOrError = WaitFor(partitionReader->Read()
                .WithTimeout(TDuration::Seconds(60)));
            readTimer.Stop();

            if (!rowsetOrError.IsOK()) {
                YT_LOG_INFO(rowsetOrError, "Finished reading due to error");
                break;
            }

            const auto& rowset = rowsetOrError.Value();

            // For compatibility with the old partition reader implementation.
            if (rowset->GetRows().empty()) {
                if (Mode_ == "online") {
                    YT_LOG_INFO("Read empty batch, skipping to next iteration");
                    continue;
                } else {
                    YT_LOG_INFO("Read empty batch, finishing up");
                    break;
                }
            }

            // Advancing offset.
            TWallTimer advanceOffsetTimer;
            auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            rowset->Commit(transaction);
            WaitFor(transaction->Commit())
                .ThrowOnError();
            advanceOffsetTimer.Stop();

            totalTimer.Stop();
            totalCpuTimer.Stop();

            statisticsCollector.ReportValues(
                readTimer.GetElapsedTime(),
                advanceOffsetTimer.GetElapsedTime(),
                totalTimer.GetElapsedTime(),
                totalCpuTimer.GetElapsedTime(),
                rowset);
        }

        statisticsCollector.LogOverallStatistics("Read all data");
    }

private:
    TString ConfigFileName_;
    TYPath ConsumerPathRaw_;
    TRichYPath ConsumerPath_;
    TYPath QueuePathRaw_;
    TRichYPath QueuePath_;
    TString Cluster_;
    i64 MaxDataWeight_;
    i64 BatchLoggingPeriod_;
    TString TabletCellBundle_;
    TString Mode_;
    IConnectionPtr Connection_;
    IClientPtr Client_;
    ISubConsumerClientPtr ConsumerClient_;
    bool UseNativeTabletNodeApi_;
    bool UsePullConsumer_;
    bool UseYTConsumers_;

    TPeriodicExecutorPtr LagReportingExecutor_;

    class TStatisticsCollector
    {
    public:
        explicit TStatisticsCollector(i64 loggingPeriod)
            : LoggingPeriod_(loggingPeriod)
        { }

        void ReportValues(
            TDuration readTime,
            TDuration advanceOffsetTime,
            TDuration totalTime,
            TDuration totalCpuTime,
            const IQueueRowsetPtr& rowset)
        {
            DataWeightRead_ += static_cast<i64>(GetDataWeight(rowset->GetRows()));

            TotalReadTime_ += readTime;
            TotalAdvanceOffsetTime_ += advanceOffsetTime;
            TotalTime_ += totalTime;

            ++SampleCount_;

            if (SampleCount_ % LoggingPeriod_ == 0) {
                YT_LOG_INFO(
                    "Iteration statistics (ReadTime: %vms, AdvanceOffsetTime: %vms, WallTime: %vms, CpuTime: %v)",
                    readTime.MillisecondsFloat(),
                    advanceOffsetTime.MillisecondsFloat(),
                    totalTime.MillisecondsFloat(),
                    totalCpuTime);

                LogOverallStatistics();
            }
        }

        void LogOverallStatistics(const TString& message = "Reporting overall statistics") const
        {
            if (SampleCount_ == 0) {
                return;
            }

            YT_LOG_INFO(
                "%v ("
                "AverageReadTime: %vms, "
                "AverageAdvanceOffsetTime: %vms, "
                "AverageWallTime: %vms, TotalWallTime: %vs, "
                "Throughput: %vMB/s, RowsetsRead: %v)",
                message,
                (TotalReadTime_/ SampleCount_).MilliSeconds(),
                (TotalAdvanceOffsetTime_ / SampleCount_).MilliSeconds(),
                (TotalTime_ / SampleCount_).MilliSeconds(),
                TotalTime_.SecondsFloat(),
                DataWeightRead_ / TotalTime_.SecondsFloat() / 1_MB,
                SampleCount_);
        }

    private:
        i64 LoggingPeriod_;
        i64 SampleCount_ = 0;
        i64 DataWeightRead_ = 0;

        TDuration TotalReadTime_ = TDuration::Zero();
        TDuration TotalAdvanceOffsetTime_ = TDuration::Zero();
        TDuration TotalTime_ = TDuration::Zero();
    };

    void AdvanceConsumerOutOfBand(i64 offset)
    {
        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        if (UseYTConsumers_) {
            transaction->AdvanceConsumer(ConsumerPath_.GetPath(), QueuePath_, 0, {}, offset);
        } else {
            transaction->AdvanceConsumer(ConsumerPath_.GetPath(), 0, {}, offset);
        }
        WaitFor(transaction->Commit())
            .ThrowOnError();
        YT_LOG_INFO("Advanced consumer out of band (Offset: %v)", offset);
    }

    void Init()
    {
        auto config = LoadConfig();

        ConfigureSingletons(config);

        Connection_ = CreateConnection(config->Connection);

        NApi::TClientOptions clientOptions;
        clientOptions.Token = *LoadToken();
        Client_ = Connection_->CreateClient(clientOptions);

        ConsumerPath_.SetPath(ConsumerPathRaw_);
        ConsumerPath_.SetCluster(Cluster_);

        QueuePath_.SetPath(QueuePathRaw_);
        QueuePath_.SetCluster(Cluster_);

        CreateConsumer();

        ConsumerClient_ = (UseYTConsumers_
            ? CreateConsumerClient(Client_, ConsumerPath_.GetPath())->GetSubConsumerClient(
                Client_, {
                    .Cluster = *QueuePath_.GetCluster(),
                    .Path = QueuePath_.GetPath(),
                })
            : CreateBigRTConsumerClient(Client_, ConsumerPath_.GetPath()));

        if (Mode_ == "online") {
            LagReportingExecutor_ = New<TPeriodicExecutor>(
                GetCurrentInvoker(),
                BIND(&TPartitionReaderBenchmarkProgram::ReportLag, MakeStrong(this)),
                TDuration::Seconds(30));
            LagReportingExecutor_->Start();

            auto latestOffset = WaitFor(FetchLatestOffset())
                .ValueOrThrow();
            AdvanceConsumerOutOfBand(latestOffset);
            YT_LOG_INFO("Advanced consumer to the front of the queue (LatestOffset: %v)", latestOffset);
        }
    }

    TFuture<i64> FetchLatestOffset()
    {
        return Client_->GetTabletInfos(QueuePath_.GetPath(), {0})
            .Apply(BIND([] (const std::vector<TTabletInfo>& tabletInfos) {
                return tabletInfos[0].TotalRowCount;
            }));
    }

    void ReportLag()
    {
        i64 nextRowIndex = 0;
        auto partitionInfos = WaitFor(ConsumerClient_->CollectPartitions(std::vector<int>{0}, /*withLastConsumeTime*/ false))
            .ValueOrThrow();
        if (!partitionInfos.empty()) {
            YT_VERIFY(partitionInfos.size() == 1);
            nextRowIndex = partitionInfos[0].NextRowIndex;
        }
        auto lastOffset = WaitFor(FetchLatestOffset())
            .ValueOrThrow();

        auto lag = lastOffset - nextRowIndex;
        YT_LOG_INFO(
            "Reporting current lag (NextRowIndex: %v, LastOffset: %v, Lag: %v)",
            nextRowIndex,
            lastOffset,
            lag);
    }

    void CreateConsumer()
    {
        // Do not recreate consumer if it already exists. We can just set the current offset to 0.
        if (WaitFor(Client_->NodeExists(ConsumerPath_.GetPath())).ValueOrThrow()) {
            AdvanceConsumerOutOfBand(0);
            return;
        }

        if (UseYTConsumers_) {
            CreateYTConsumer();
            WaitFor(Client_->RegisterQueueConsumer(QueuePath_, ConsumerPath_, /*vital*/ false))
                .ThrowOnError();
            // Give registration caches time to sync.
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(30));
        } else {
            CreateBigRTConsumer();
        }

        WaitFor(Client_->SetNode(ConsumerPath_.GetPath() + "/@treat_as_queue_consumer", ConvertToYsonString(true)))
            .ThrowOnError();
    }

    void CreateBigRTConsumer()
    {
        TCreateNodeOptions options;
        options.Force = true;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("dynamic", true);
        options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("ShardId", EValueType::Uint64, ESortOrder::Ascending),
            TColumnSchema("Offset", EValueType::Uint64),
        }, /*strict*/ true, /*uniqueKeys*/ true));
        options.Attributes->Set("tablet_cell_bundle", TabletCellBundle_);

        WaitFor(Client_->CreateNode(ConsumerPath_.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        Client_->MountTable(ConsumerPath_.GetPath());
        WaitUntil(ConsumerPath_.GetPath() + "/@tablet_state", "mounted");

        // TODO(achulkov2): Use TCrossClusterReference.
        WaitFor(Client_->SetNode(ConsumerPath_.GetPath() + "/@target_queue", ConvertToYsonString(Format("%v:%v", Cluster_, QueuePath_))))
            .ThrowOnError();

        YT_LOG_DEBUG("Created consumer (Path: %v)", ConsumerPath_);
    }

    void CreateYTConsumer()
    {
        TCreateNodeOptions options;
        options.Force = true;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("dynamic", true);
        options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("partition_index", EValueType::Uint64, ESortOrder::Ascending).SetRequired(true),
            TColumnSchema("offset", EValueType::Uint64).SetRequired(true),
        }, /*strict*/ true, /*uniqueKeys*/ true));
        options.Attributes->Set("tablet_cell_bundle", TabletCellBundle_);

        WaitFor(Client_->CreateNode(ConsumerPath_.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        Client_->MountTable(ConsumerPath_.GetPath());
        WaitUntil(ConsumerPath_.GetPath() + "/@tablet_state", "mounted");

        YT_LOG_DEBUG("Created consumer (Path: %v)", ConsumerPath_);
    }

    void WaitUntil(const TYPath& path, const TString& expected)
    {
        TWallTimer timer;
        bool reached = false;
        for (int attempt = 0; attempt < 60; ++attempt) {
            auto state = WaitFor(Client_->GetNode(path))
                .ValueOrThrow();
            auto value = ConvertTo<IStringNodePtr>(state)->GetValue();
            if (value == expected) {
                reached = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(300));
        }

        if (!reached) {
            THROW_ERROR_EXCEPTION(
                "%Qv is not %Qv after %v",
                path,
                expected,
                timer.GetElapsedTime());
        }
    }

    TConfigPtr LoadConfig()
    {
        auto config = New<TConfig>();

        if (!ConfigFileName_) {
            return config;
        }

        TIFStream stream(ConfigFileName_);
        auto node = ConvertToNode(&stream);
        config->Load(node);
        return config;
    }
};

void Main(int argc, const char** argv)
{
    TPartitionReaderBenchmarkProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv) {
    auto actionQueue = NYT::New<NYT::NConcurrency::TActionQueue>();
    NYT::NConcurrency::WaitFor(
        BIND(&NYT::Main)
        .AsyncVia(actionQueue->GetInvoker())
        .Run(argc, argv))
        .ThrowOnError();

    actionQueue->Shutdown();
}
