#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/server/lib/misc/archive_reporter.h>

#include <yt/yt/ytlib/controller_agent/structs.h>

#include <yt/yt/ytlib/scheduler/records/operation_events.record.h>

#include <yt/yt/client/api/operation_client.h>

#include <yt/yt/client/table_client/record_helpers.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TOperationEventReport
{
    NJobTrackerClient::TOperationId OperationId;
    TInstant Timestamp;
    NApi::EOperationEventType EventType;
    std::optional<std::string> Incarnation;
    std::optional<NControllerAgent::EOperationIncarnationSwitchReason> IncarnationSwitchReason;
    std::optional<NControllerAgent::TIncarnationSwitchInfo> IncarnationSwitchInfo;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationEventRowlet
    : public IArchiveRowlet
{
public:
    explicit TOperationEventRowlet(TOperationEventReport&& report);

    size_t EstimateSize() const override;

    NTableClient::TUnversionedOwningRow ToRow(int archiveVersion) const override;

private:
    const TOperationEventReport Report_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationEventReporter
    : public TRefCounted
{
public:
    TOperationEventReporter(
        TOperationEventReporterConfigPtr config,
        NApi::NNative::IConnectionPtr connection);

    void ReportEvent(TOperationEventReport event);

    void SetOperationsArchiveVersion(int version);

private:
    const NConcurrency::TActionQueuePtr OperationEventsReporterActionQueue_ = New<NConcurrency::TActionQueue>("OperationEvents");
    const TOperationEventReporterConfigPtr Config_;
    const TArchiveVersionHolderPtr ArchiveVersion_ = New<TArchiveVersionHolder>();
    const IArchiveReporterPtr ArchiveReporter_;
};

DEFINE_REFCOUNTED_TYPE(TOperationEventReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
