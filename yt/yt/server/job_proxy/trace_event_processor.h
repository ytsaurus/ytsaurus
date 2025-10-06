#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/archive_reporter.h>

#include <yt/yt/ytlib/scheduler/records/job_trace_event.record.h>
#include <yt/yt/ytlib/scheduler/records/job_trace.record.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/table_output.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/formats/parser.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TOperationId;
using NJobTrackerClient::TJobId;
using NJobTrackerClient::TJobTraceId;

////////////////////////////////////////////////////////////////////////////////

using TProcessId = int;

////////////////////////////////////////////////////////////////////////////////

struct TJobTraceEventReport
{
    const TOperationId OperationId;
    const TJobId JobId;
    const TJobTraceId TraceId;
    const i64 EventIndex;
    const std::string Event;
    const double EventTime;
};

struct TJobTraceReport
{
    const TOperationId OperationId;
    const TJobId JobId;
    const TJobTraceId TraceId;
    const NJobTrackerClient::EJobTraceState State;
    const TProcessId ProcessId;
};

////////////////////////////////////////////////////////////////////////////////

class TJobTraceEventRowlet
    : public NServer::IArchiveRowlet
{
public:
    TJobTraceEventRowlet(TJobTraceEventReport&& report);

    size_t EstimateSize() const override;

    NTableClient::TUnversionedOwningRow ToRow(int archiveVersion) const override;

private:
    const TJobTraceEventReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobTraceRowlet
    : public NServer::IArchiveRowlet
{
public:
    TJobTraceRowlet(TJobTraceReport&& report);

    size_t EstimateSize() const override;

    NTableClient::TUnversionedOwningRow ToRow(int archiveVersion) const override;

private:
    const TJobTraceReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

struct TTraceEvent
{
    const std::string RawEvent;
    const i64 ThreadId;
    const double Timestamp;
    const std::optional<TProcessId> ProcessId;
};

struct TTraceControlEvent
{
    const NJobTrackerClient::EJobTraceEventType Type;
    const TJobTraceId TraceId;
    const TProcessId ProcessId;
};

////////////////////////////////////////////////////////////////////////////////

class TJobTraceEventProcessor
    : public TRefCounted
{
public:
    TJobTraceEventProcessor(
        TJobTraceEventProcessorConfigPtr config,
        const NApi::NNative::IConnectionPtr& connection,
        TOperationId operationId,
        TJobId jobId,
        std::optional<int> operationsArchiveVersion);

    void OnEvent(const TTraceEvent& event);
    void OnControlEvent(const TTraceControlEvent& controlEvent);

    void FinishGlobalTrace();

private:
    const TOperationId OperationId_;
    const TJobId JobId_;

    THashMap<TJobTraceId, int> TraceIdToPidCount_;
    THashMap<TProcessId, TJobTraceId> PidToTraceId_;

    i64 EventIndex_ = 0;
    const TJobTraceEventProcessorConfigPtr Config_;

    // NB(bystrovserg): There are two ways to obtain a trace ID: receive it via control events,
    // or have the job proxy generate a single trace ID for the entire trace.
    bool HasControlEvents_ = false;
    TJobTraceId GlobalTraceId_;

    const NConcurrency::TActionQueuePtr JobTraceReporterActionQueue_ = New<NConcurrency::TActionQueue>("JobTrace");
    const NServer::TArchiveVersionHolderPtr ArchiveVersion_ = New<NServer::TArchiveVersionHolder>();

    const NServer::IArchiveReporterPtr JobTraceEventReporter_;
    const NServer::IArchiveReporterPtr JobTraceReporter_;

    void OnTraceStartedForPid(TProcessId processId, TJobTraceId traceId);
    void OnTraceFinishedForPid(TProcessId processId, TJobTraceId traceId, NJobTrackerClient::EJobTraceState traceState);

    void ReportTraceState(TProcessId processId, TJobTraceId traceId, NJobTrackerClient::EJobTraceState traceState);

    TJobTraceId GetTraceId(const TTraceEvent& event);
};

DEFINE_REFCOUNTED_TYPE(TJobTraceEventProcessor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
