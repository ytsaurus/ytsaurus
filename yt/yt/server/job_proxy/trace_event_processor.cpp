#include "trace_event_processor.h"

#include "private.h"

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/estimate_size_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/options.h>

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

constinit const auto Logger = JobTraceLogger;

constexpr TProcessId NullProcessId = -1;

////////////////////////////////////////////////////////////////////////////////

EJobTraceState ConvertControlEventTypeToTraceState(EJobTraceEventType eventType)
{
    switch (eventType) {
        case EJobTraceEventType::TraceStarted:
            return EJobTraceState::Started;
        case EJobTraceEventType::TraceFinished:
            return EJobTraceState::Finished;
        case EJobTraceEventType::TraceDropped:
            return EJobTraceState::Dropped;
    }
}

////////////////////////////////////////////////////////////////////////////////

TJobTraceEventRowlet::TJobTraceEventRowlet(TJobTraceEventReport&& report)
    : Report_(std::move(report))
{ }

size_t TJobTraceEventRowlet::EstimateSize() const
{
    return NServer::EstimateSizes(
        Report_.OperationId,
        Report_.JobId,
        Report_.TraceId,
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

TJobTraceRowlet::TJobTraceRowlet(TJobTraceReport&& report)
    : Report_(std::move(report))
{ }

size_t TJobTraceRowlet::EstimateSize() const
{
    return NServer::EstimateSizes(
        Report_.OperationId,
        Report_.JobId,
        Report_.TraceId,
        Report_.State,
        Report_.ProcessId);
}

TUnversionedOwningRow TJobTraceRowlet::ToRow(int /*archiveVersion*/) const
{
    auto operationIdAsGuid = Report_.OperationId.Underlying();
    auto jobIdAsGuid = Report_.JobId.Underlying();
    auto traceIdAsGuid = Report_.TraceId.Underlying();
    NScheduler::NRecords::TJobTracePartial record{
        .Key{
            .OperationIdHi = operationIdAsGuid.Parts64[0],
            .OperationIdLo = operationIdAsGuid.Parts64[1],
            .JobIdHi = jobIdAsGuid.Parts64[0],
            .JobIdLo = jobIdAsGuid.Parts64[1],
            .TraceIdHi = traceIdAsGuid.Parts64[0],
            .TraceIdLo = traceIdAsGuid.Parts64[1],
            .ProcessId = Report_.ProcessId,
        },
        .State = FormatEnum(Report_.State),
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
    , Config_(std::move(config))
    , GlobalTraceId_(TGuid::Create())
    , JobTraceEventReporter_(
        CreateArchiveReporter(
            ArchiveVersion_,
            Config_->Reporter,
            Config_->Reporter->JobTraceEventHandler,
            NScheduler::NRecords::TJobTraceEventDescriptor::Get()->GetNameTable(),
            "job_trace_events",
            connection->CreateNativeClient(
                NApi::NNative::TClientOptions::FromUser(Config_->Reporter->User)),
            JobTraceReporterActionQueue_->GetInvoker(),
            ReporterProfiler.WithTag("reporter_type", "job_trace_events")))
    , JobTraceReporter_(
        CreateArchiveReporter(
            ArchiveVersion_,
            Config_->Reporter,
            Config_->Reporter->JobTraceHandler,
            NScheduler::NRecords::TJobTraceDescriptor::Get()->GetNameTable(),
            "job_traces",
            connection->CreateNativeClient(
                NApi::NNative::TClientOptions::FromUser(Config_->Reporter->User)),
            JobTraceReporterActionQueue_->GetInvoker(),
            ReporterProfiler.WithTag("reporter_type", "job_traces")))
{
    if (operationsArchiveVersion) {
        ArchiveVersion_->Set(*operationsArchiveVersion);
    }
}

void TJobTraceEventProcessor::OnEvent(const TTraceEvent& event)
{
    // COMPAT(bystrovserg)
    if (ArchiveVersion_->Get() < 53) {
        return;
    }

    auto traceId = GetTraceId(event);
    if (traceId == GlobalTraceId_ && EventIndex_ == 0) {
        ReportTraceState(NullProcessId, GlobalTraceId_, EJobTraceState::Started);
    }
    i64 eventIndex = EventIndex_++;

    auto report = TJobTraceEventReport{
        .OperationId = OperationId_,
        .JobId = JobId_,
        .TraceId = traceId,
        .EventIndex = eventIndex,
        .Event = std::move(event.RawEvent),
        .EventTime = event.Timestamp,
    };

    if (eventIndex % Config_->LoggingInterval == 0) {
        YT_LOG_DEBUG(
            "Job trace event processor statistics "
            "(TraceId: %v, EventCount: %v, ActiveProcessIds: %v)",
            traceId,
            eventIndex,
            GetKeys(PidToTraceId_));
    }

    auto rowlet = std::make_unique<TJobTraceEventRowlet>(std::move(report));
    JobTraceEventReporter_->Enqueue(std::move(rowlet));
}

void TJobTraceEventProcessor::FinishGlobalTrace()
{
    if (EventIndex_ > 0 && !HasControlEvents_) {
        YT_LOG_DEBUG("Report the end of global trace (TraceId: %v)", GlobalTraceId_);

        ReportTraceState(NullProcessId, GlobalTraceId_, EJobTraceState::Finished);
    }
}

void TJobTraceEventProcessor::OnControlEvent(const TTraceControlEvent& event)
{
    HasControlEvents_ = true;
    auto traceId = event.TraceId;
    auto pid = event.ProcessId;
    auto type = event.Type;

    YT_LOG_DEBUG("Received trace control event (Type: %v, TraceId: %v, Pid: %v)",
        type,
        traceId,
        pid);

    switch (type) {
        case EJobTraceEventType::TraceStarted:
            OnTraceStartedForPid(pid, traceId);
            break;
        case EJobTraceEventType::TraceFinished:
        case EJobTraceEventType::TraceDropped:
            OnTraceFinishedForPid(pid, traceId, ConvertControlEventTypeToTraceState(type));
            break;
    }
}

TJobTraceId TJobTraceEventProcessor::GetTraceId(const TTraceEvent& event)
{
    // NB(bystrovserg): Without control events we generate one trace ID for the entire trace.
    if (!HasControlEvents_) {
        return GlobalTraceId_;
    }

    if (PidToTraceId_.empty()) {
        THROW_ERROR_EXCEPTION("There are no traces currently being written, but the job has used control trace events");
    }

    if (!event.ProcessId) {
        // NB(bystrovserg): If we don't have PID, we choose some available trace ID
        // because they shouldn't differ in most cases.
        // TODO(bystrovserg): Rather fix stuck trace IDs or find a way for choosing active traces.
        return PidToTraceId_.begin()->second;
    }

    auto it = PidToTraceId_.find(*event.ProcessId);
    if (it == PidToTraceId_.end()) {
        // NB(bystrovserg): We allow not to have trace ID for pid, in this case we choose some available trace ID.
        return PidToTraceId_.begin()->second;
    }

    return it->second;
}

void TJobTraceEventProcessor::OnTraceStartedForPid(TProcessId pid, TJobTraceId traceId)
{
    if (auto it = PidToTraceId_.find(pid); it != PidToTraceId_.end()) {
        auto currentTraceId = it->second;
        if (currentTraceId == traceId) {
            THROW_ERROR_EXCEPTION("Received several \"trace_started\" events for trace")
                << TErrorAttribute("trace_id", traceId)
                << TErrorAttribute("pid", pid);
        }

        YT_LOG_ERROR("Did not receive finish event for previous trace (ProcessId: %v, PreviousTraceId: %v, NewTraceId: %v)",
            pid,
            currentTraceId,
            traceId);

        OnTraceFinishedForPid(pid, currentTraceId, EJobTraceState::Orphaned);
    }

    PidToTraceId_[pid] = traceId;

    YT_LOG_DEBUG("Trace has started for process (TraceId: %v, Pid: %v)",
        traceId,
        pid);

    {
        auto pidCountIt = TraceIdToPidCount_.find(traceId);
        if (pidCountIt == TraceIdToPidCount_.end()) {
            YT_LOG_DEBUG("Trace has started (TraceId: %v)", traceId);

            pidCountIt = EmplaceOrCrash(TraceIdToPidCount_, traceId, 0);
        }

        auto& pidCount = pidCountIt->second;
        ++pidCount;
    }

    // COMPAT(bystrovserg)
    if (ArchiveVersion_->Get() < 63) {
        return;
    }

    ReportTraceState(pid, traceId, EJobTraceState::Started);
}

void TJobTraceEventProcessor::OnTraceFinishedForPid(TProcessId pid, TJobTraceId traceId, EJobTraceState traceState)
{
    auto traceIdIt = PidToTraceId_.find(pid);
    if (traceIdIt == PidToTraceId_.end()) {
        THROW_ERROR_EXCEPTION("Received finish event without start event")
            << TErrorAttribute("trace_id", traceId)
            << TErrorAttribute("pid", pid);
    }

    PidToTraceId_.erase(traceIdIt);

    YT_LOG_DEBUG("Trace has finished for process (TraceId: %v, Pid: %v)",
        traceId,
        pid);

    // NB(bystrovserg): TraceIdToPidCount_ and PidToTraceId_ must have keys at the same time, so we do not check here.
    auto pidCountIt = GetIteratorOrCrash(TraceIdToPidCount_, traceId);
    auto pidCount = pidCountIt->second;
    if (pidCount == 0) {
        YT_LOG_DEBUG("Trace has finished (TraceId: %v)", traceId);

        TraceIdToPidCount_.erase(pidCountIt);
    }

    // COMPAT(bystrovserg)
    if (ArchiveVersion_->Get() < 63) {
        return;
    }

    ReportTraceState(pid, traceId, traceState);
}

void TJobTraceEventProcessor::ReportTraceState(TProcessId processId, TJobTraceId traceId, EJobTraceState traceState)
{
    auto report = TJobTraceReport{
        .OperationId = OperationId_,
        .JobId = JobId_,
        .TraceId = traceId,
        .State = traceState,
        .ProcessId = processId,
    };

    auto rowlet = std::make_unique<TJobTraceRowlet>(std::move(report));
    JobTraceReporter_->Enqueue(std::move(rowlet));
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
