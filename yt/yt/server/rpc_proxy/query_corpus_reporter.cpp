#include "query_corpus_reporter.h"

#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/profiling/timing.h>

namespace NYT::NRpcProxy {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyLogger();

static const auto QueryCorpusTableSchema = TTableSchema({{"query", EValueType::String, ESortOrder::Ascending}});
static const auto QueryCorpusNameTable = TNameTable::FromSchema(QueryCorpusTableSchema);
static const int QueryCorpusColumnCount = QueryCorpusNameTable->GetSize();

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TQueryCorpusReporter)

class TQueryCorpusReporter
    : public IQueryCorpusReporter
{
public:
    explicit TQueryCorpusReporter(NApi::NNative::IClientPtr client)
        : Client_(std::move(client))
        , ActionQueue_(New<TActionQueue>("QueryCorpusReporter"))
        , Executor_(New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TQueryCorpusReporter::ReportStatistics, MakeWeak(this))))
        , Profiler_(RpcProxyProfiler().WithPrefix("/query_corpus_reporter"))
        , ReportCount_(Profiler_.Counter("/report_count"))
        , ReportErrorCount_(Profiler_.Counter("/report_error_count"))
        , ReportTime_(Profiler_.Timer("/report_time"))
    { }

    void Reconfigure(const TQueryCorpusReporterConfigPtr& config) override
    {
        bool enableChanged = Enable_ != config->Enable;

        Enable_ = config->Enable;
        MaxBatchSize_ = config->MaxBatchSize;
        Config_.Exchange(New<TConfig>(config->TablePath, config->ReportBackoffTime));

        Executor_->SetOptions(TPeriodicExecutorOptions{config->Period, config->Splay, config->Jitter});

        if (enableChanged) {
            if (Enable_) {
                Executor_->Start();
            } else {
                YT_UNUSED_FUTURE(Executor_->Stop());
            }
        }
    }

    void AddQuery(TStringBuf query) override
    {
        if (!Enable_) {
            return;
        }

        if (BatchSize_ > MaxBatchSize_) {
            return;
        }

        Batch_.Enqueue(TString(query));
        ++BatchSize_;
    }

private:
    struct TConfig final
    {
        std::optional<NYPath::TYPath> TablePath;
        TDuration ReportBackoffTime;
    };

    const NApi::NNative::IClientPtr Client_;

    std::atomic<bool> Enable_ = false;
    TAtomicIntrusivePtr<TConfig> Config_;
    TMpscStack<TString> Batch_;
    std::atomic<int> BatchSize_;
    std::atomic<int> MaxBatchSize_;

    NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr Executor_;

    TProfiler Profiler_;
    TCounter ReportCount_;
    TCounter ReportErrorCount_;
    TEventTimer ReportTime_;

    void ReportStatistics()
    {
        if (!Enable_) {
            return;
        }

        auto context = CreateTraceContextFromCurrent("QueryCorpusReporter");
        auto contextGuard = TTraceContextGuard(context);

        auto batch = Batch_.DequeueAll();
        BatchSize_ -= batch.size();

        YT_LOG_DEBUG("Started reporting queries for corpus");
        auto timer = TWallTimer();

        try {
            DoReportStatistics(Config_.Acquire()->TablePath, batch);
            ReportCount_.Increment();
            YT_LOG_DEBUG("Finished reporting queries for corpus");
        } catch (const std::exception& ex) {
            ReportErrorCount_.Increment();
            YT_LOG_ERROR(ex, "Failed to report queries for corpus");
            timer.Stop();
            TDelayedExecutor::WaitForDuration(Config_.Acquire()->ReportBackoffTime);
        }

        ReportTime_.Record(timer.GetElapsedTime());
    }

    void DoReportStatistics(const std::optional<TYPath>& tablePath, const std::vector<TString>& batch)
    {
        if (!tablePath || batch.empty()) {
            return;
        }

        auto rowBuffer = New<TRowBuffer>();

        auto rows = rowBuffer->GetPool()->AllocateUninitialized<TUnversionedRow>(std::ssize(batch));

        for (int i = 0; i < std::ssize(batch); ++i) {
            auto row = rowBuffer->AllocateUnversioned(QueryCorpusColumnCount);
            row[0] = MakeUnversionedStringValue(batch[i], 0);
            rows[i] = row;
        }

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            *tablePath,
            QueryCorpusNameTable,
            MakeSharedRange(TRange(rows, std::ssize(batch)), rowBuffer));

        WaitFor(transaction->Commit())
            .ThrowOnError();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryCorpusReporter);

////////////////////////////////////////////////////////////////////////////////

IQueryCorpusReporterPtr MakeQueryCorpusReporter(NApi::NNative::IClientPtr client)
{
    return New<TQueryCorpusReporter>(std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
