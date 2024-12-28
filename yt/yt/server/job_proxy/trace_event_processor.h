#pragma once

#include "public.h"

#include <yt/yt/ytlib/scheduler/records/job_trace_event.record.h>

#include <yt/yt/server/lib/misc/archive_reporter.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/table_output.h>

#include <yt/yt/client/formats/parser.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TJobTraceId, TGuid);

////////////////////////////////////////////////////////////////////////////////

struct TJobTraceEventReport
{
    NJobTrackerClient::TOperationId OperationId;
    NJobTrackerClient::TJobId JobId;
    TJobTraceId TraceId;
    int EventIndex = 0;
    TString Event;
    double EventTime = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TJobTraceEventRowlet
    : public NServer::IArchiveRowlet
{
public:
    TJobTraceEventRowlet(TJobTraceEventReport&& report);

    size_t EstimateSize() const override;

    NTableClient::TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override;

private:
    const TJobTraceEventReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

struct TTraceEvent
{
    TString RawEvent;
    int ThreadId = 0;
    double Timestamp = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TJobTraceEventProcessor
    : public TRefCounted
{
public:
    TJobTraceEventProcessor(
        TJobTraceEventProcessorConfigPtr config,
        const NApi::NNative::IConnectionPtr& connection,
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        std::optional<int> operationsArchiveVersion);

    void OnEvent(TTraceEvent event);

private:
    const NJobTrackerClient::TOperationId OperationId_;
    const NJobTrackerClient::TJobId JobId_;
    TJobTraceId TraceId_;
    int EventIndex_ = 0;

    const TJobTraceEventProcessorConfigPtr Config_;

    const NConcurrency::TActionQueuePtr JobTraceReporterActionQueue_ = New<NConcurrency::TActionQueue>("JobTrace");
    const NServer::TArchiveVersionHolderPtr ArchiveVersion_ = New<NServer::TArchiveVersionHolder>();

    const NServer::IArchiveReporterPtr ArchiveReporter_;

    bool IsControlEvent(const TTraceEvent& event) const;
};

DEFINE_REFCOUNTED_TYPE(TJobTraceEventProcessor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
