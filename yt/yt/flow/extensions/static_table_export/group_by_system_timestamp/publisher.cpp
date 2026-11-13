#include "publisher.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int MaxWindowsPerIteration = 16;

} // namespace

std::optional<TSystemTimestamp> GetStaticTableExportStart(const TStaticTableExportPublisherInput& input)
{
    // Partitions idle at startup move the watermark to the current time, so it cannot bound the start.
    if (input.PartitionCount == 0 || input.FirstWriteReportCount + input.SilentDrainingCount < input.PartitionCount) {
        return std::nullopt;
    }
    // A finite source with nothing to read still bootstraps, since its completing jobs wait for it.
    if (!input.FirstWriteTimestamp && input.SilentCompletingCount == input.PartitionCount) {
        return TSystemTimestamp(TInstant::Now().Seconds());
    }
    // A silently draining partition read nothing, so its older backlog later goes to the windows after the start.
    // Null when every partition drains silently: there is nothing to export yet.
    return input.FirstWriteTimestamp;
}

TStaticTableExportPublisher::TStaticTableExportPublisher(
    TStaticTableExportWriterStoragePtr storage,
    TStaticTableExportParametersPtr parameters,
    TBuildStaticTableExportPublication buildPublication,
    IInvokerPtr invoker,
    IStatusErrorStatePtr errorState,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : Storage_(std::move(storage))
    , Parameters_(std::move(parameters))
    , BuildPublication_(std::move(buildPublication))
    , ErrorState_(std::move(errorState))
    , Logger(logger)
    , ClosedThrough_(profiler.Gauge("/closed_through"))
    , Executor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND(&TStaticTableExportPublisher::Publish, MakeWeak(this)),
        TDuration::Seconds(1)))
{
    Executor_->Start();
}

TStaticTableExportPublisher::~TStaticTableExportPublisher()
{
    YT_UNUSED_FUTURE(Executor_->Stop());
}

void TStaticTableExportPublisher::Update(TStaticTableExportPublisherInput input)
{
    if (input.Dynamic) {
        Executor_->SetPeriod(input.Dynamic->PublicationPeriod);
    }
    auto guard = Guard(Lock_);
    Input_ = std::move(input);
}

void TStaticTableExportPublisher::Publish()
{
    const auto input = [&] {
        auto guard = Guard(Lock_);
        return Input_;
    }();
    if (!input.Dynamic || input.PartitionCount == 0 || TInstant::Now() < RetryAfter_) {
        return;
    }
    const auto period = Parameters_->WindowPeriod.Seconds();
    std::optional<ui64> window;
    try {
        auto progress = Storage_->ReadProgress();
        if (!progress) {
            // Writers admit nothing before bootstrap, so no message older than the start is acknowledged.
            const auto start = GetStaticTableExportStart(input);
            if (!start) {
                return;
            }
            const auto closedThrough = GetStaticTableExportWindow(*start, Parameters_->WindowPeriod) - period;
            YT_TLOG_INFO("Bootstrapping static table export")
                .With("ClosedThrough", closedThrough);
            Storage_->Bootstrap(input.PartitionCount, closedThrough);
            return;
        }
        THROW_ERROR_EXCEPTION_UNLESS(progress->PartitionCount == input.PartitionCount,
            "Static table export source partition count changed from %v to %v",
            progress->PartitionCount,
            input.PartitionCount);
        // Writers of the current schema wait for the migration, so it cannot wait for a closable window.
        if (!Storage_->HasCurrentSchema(progress)) {
            if (!Storage_->MigrateSchema()) {
                RetryAfter_ = TInstant::Now() + input.Dynamic->RetryBackoff;
                return;
            }
            YT_TLOG_INFO("Migrated static table export gates to the current schema");
        }
        auto closedThrough = progress->ClosedThrough;
        for (int index = 0; index < MaxWindowsPerIteration; ++index) {
            window = closedThrough + period;
            const bool closable = IsStaticTableExportWindowClosable(*window, input.EventWatermark, Parameters_);
            if (!closable && !input.AllDrained) {
                break;
            }
            // A drained export publishes its written tail but no empty windows beyond it.
            const auto result = Storage_->PublishNext(BuildPublication_, /*tailOnly*/ !closable, input.Dynamic->TableTtl);
            if (result == EStaticTableExportPublishResult::Busy) {
                RetryAfter_ = TInstant::Now() + input.Dynamic->RetryBackoff;
                break;
            }
            if (result == EStaticTableExportPublishResult::EmptyTail) {
                break;
            }
            closedThrough = *window;
            ClosedThrough_.Update(closedThrough);
            YT_TLOG_INFO("Published static table export window")
                .With("Window", *window);
        }
        ErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to publish static table export window")
            .With("window", window)
            .With(TError(ex));
        YT_TLOG_WARNING("Static table export publication failed")
            .With(error);
        ErrorState_->SetError(error);
        RetryAfter_ = TInstant::Now() + input.Dynamic->RetryBackoff;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
