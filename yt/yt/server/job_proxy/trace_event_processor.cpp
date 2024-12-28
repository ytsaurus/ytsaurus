#include "trace_event_processor.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/estimate_size_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/json/json_parser.h>

namespace NYT::NJobProxy {

using namespace NJobTrackerClient;
using namespace NJson;
using namespace NYTree;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler ReporterProfiler("/job_trace_event_reporter");

static const NLogging::TLogger Logger("JobTraceProcessor");

static const int ControlEventThreadId = -1;

////////////////////////////////////////////////////////////////////////////////

TJobTraceEventRowlet::TJobTraceEventRowlet(TJobTraceEventReport&& report)
    : Report_(std::move(report))
{ }

size_t TJobTraceEventRowlet::EstimateSize() const
{
    return NServer::EstimateSizes(
        Report_.OperationId.Underlying(),
        Report_.JobId.Underlying(),
        Report_.TraceId.Underlying(),
        Report_.EventIndex,
        Report_.Event,
        Report_.EventTime);
}

TUnversionedOwningRow TJobTraceEventRowlet::ToRow(int /*archiveVersion*/) const
{
    auto operationIdAsGuid = Report_.OperationId.Underlying();
    auto jobIdAsGuid = Report_.JobId.Underlying();
    auto traceIdAsGuid = Report_.TraceId.Underlying();
    NScheduler::NRecords::TJobTraceEventPartial record{
        .Key{
            .OperationIdHi = operationIdAsGuid.Parts64[0],
            .OperationIdLo = operationIdAsGuid.Parts64[1],
            .JobIdHi = jobIdAsGuid.Parts64[0],
            .JobIdLo = jobIdAsGuid.Parts64[1],
            .TraceIdHi = traceIdAsGuid.Parts64[0],
            .TraceIdLo = traceIdAsGuid.Parts64[1],
            .EventIndex = Report_.EventIndex,
        },
        .Event = Report_.Event,
        .EventTime = Report_.EventTime,
    };

    return FromRecord(record);
}

////////////////////////////////////////////////////////////////////////////////

TJobTraceEventProcessor::TJobTraceEventProcessor(
    TJobTraceEventProcessorConfigPtr config,
    const NApi::NNative::IConnectionPtr& connection,
    TOperationId operationId,
    TJobId jobId,
    std::optional<int> operationsArchiveVersion)
    : OperationId_(operationId)
    , JobId_(jobId)
    , TraceId_(TGuid::Create())
    , Config_(std::move(config))
    , ArchiveReporter_(
        CreateArchiveReporter(
            ArchiveVersion_,
            Config_->Reporter,
            Config_->Reporter->JobTraceEventHandler,
            NScheduler::NRecords::TJobTraceEventDescriptor::Get()->GetNameTable(),
            "job_trace_events",
            connection->CreateNativeClient(
                NApi::TClientOptions::FromUser(Config_->Reporter->User)),
            JobTraceReporterActionQueue_->GetInvoker(),
            ReporterProfiler.WithTag("reporter_type", "job_trace_events")))
{
    if (operationsArchiveVersion) {
        ArchiveVersion_->Set(*operationsArchiveVersion);
    }
}

bool TJobTraceEventProcessor::IsControlEvent(const TTraceEvent& event) const
{
    return event.ThreadId == ControlEventThreadId;
}

void TJobTraceEventProcessor::OnEvent(TTraceEvent event)
{
    // COMPAT(omgronny)
    if (ArchiveVersion_->Get() < 53) {
        return;
    }

    // We don't want to generate new trace id on the finish control event.
    if (IsControlEvent(event) && EventIndex_ != 0) {
        TraceId_ = TJobTraceId(TGuid::Create());
        EventIndex_ = 0;

        YT_LOG_DEBUG("New trace started (OperationId: %v, JobId: %v, TraceId: %v)",
            OperationId_,
            JobId_,
            TraceId_);
    }

    auto report = TJobTraceEventReport{
        .OperationId = OperationId_,
        .JobId = JobId_,
        .TraceId = TraceId_,
        .EventIndex = EventIndex_++,
        .Event = std::move(event.RawEvent),
        .EventTime = event.Timestamp,
    };

    if (EventIndex_ % Config_->LoggingInterval == 0) {
        YT_LOG_DEBUG("Job trace event processor statistics "
            "(OperationId: %v, JobId: %v, TraceId: %v, EventCount: %v)",
            OperationId_,
            JobId_,
            TraceId_,
            EventIndex_);
    }

    auto rowlet = std::make_unique<TJobTraceEventRowlet>(std::move(report));
    ArchiveReporter_->Enqueue(std::move(rowlet));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
