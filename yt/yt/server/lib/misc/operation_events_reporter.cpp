#include "operation_events_reporter.h"

#include <yt/yt/server/lib/misc/estimate_size_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NServer {

using namespace NTabletClient;
using namespace NControllerAgent;
using namespace NYson;
using namespace NYTree;
using namespace NScheduler;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler ReporterProfiler("/operation_event_reporter");
static const NLogging::TLogger Logger("OperationEventReporter");

////////////////////////////////////////////////////////////////////////////////

TOperationEventRowlet::TOperationEventRowlet(TOperationEventReport&& report)
    : Report_(std::move(report))
{ }

size_t TOperationEventRowlet::EstimateSize() const
{
    // TODO(bystrovserg): Use tranform from std::optional after switching to C++23.
    std::optional<TYsonString> incarnationSwitchInfoYson;
    if (Report_.IncarnationSwitchInfo) {
        incarnationSwitchInfoYson = ConvertToYsonString(*Report_.IncarnationSwitchInfo);
    }

    return EstimateSizes(
        Report_.OperationId,
        Report_.Timestamp,
        Report_.EventType,
        Report_.Incarnation,
        Report_.IncarnationSwitchReason,
        incarnationSwitchInfoYson);
}

NTableClient::TUnversionedOwningRow TOperationEventRowlet::ToRow(int /*archiveVersion*/) const
{
    auto operationIdAsGuid = Report_.OperationId.Underlying();

    // TODO(bystrovserg): Use tranform from std::optional after switching to C++23.
    std::optional<std::string> incarnationSwitchReason;
    if (Report_.IncarnationSwitchReason) {
        incarnationSwitchReason = FormatEnum(*Report_.IncarnationSwitchReason);
    }
    std::optional<TYsonString> incarnationSwitchInfoYson;
    if (Report_.IncarnationSwitchInfo) {
        incarnationSwitchInfoYson = ConvertToYsonString(*Report_.IncarnationSwitchInfo);
    }

    NRecords::TOperationEventPartial record{
        .Key{
            .OperationIdHi = operationIdAsGuid.Parts64[0],
            .OperationIdLo = operationIdAsGuid.Parts64[1],
            .Timestamp = Report_.Timestamp,
            .EventType = FormatEnum(Report_.EventType),
        },
        .Incarnation = Report_.Incarnation,
        .IncarnationSwitchReason = incarnationSwitchReason,
        .IncarnationSwitchInfo = incarnationSwitchInfoYson,
    };

    return NTableClient::FromRecord(record);
}

////////////////////////////////////////////////////////////////////////////////

TOperationEventReporter::TOperationEventReporter(
    TOperationEventReporterConfigPtr config,
    NApi::NNative::IConnectionPtr connection)
    : Config_(std::move(config))
    , ArchiveReporter_(
        NServer::CreateArchiveReporter(
            ArchiveVersion_,
            Config_,
            Config_->Handler,
            NRecords::TOperationEventDescriptor::Get()->GetNameTable(),
            "operation_events",
            connection->CreateNativeClient(
                NApi::TClientOptions::FromUser(Config_->User)),
            OperationEventsReporterActionQueue_->GetInvoker(),
            ReporterProfiler.WithTag("reporter_type", "operation_events")))
{ }

void TOperationEventReporter::SetOperationsArchiveVersion(int version)
{
    ArchiveVersion_->Set(version);
}

void TOperationEventReporter::ReportEvent(TOperationEventReport event)
{
    // COMPAT(bystrovserg)
    if (event.EventType == EOperationEventType::IncarnationStarted && ArchiveVersion_->Get() < 59) {
        return;
    }

    if (!Config_->Enabled) {
        YT_LOG_DEBUG("Operation event reporter disabled (OperationId: %v, EventType: %v)",
            event.OperationId,
            event.EventType);
        return;
    }

    auto rowlet = std::make_unique<TOperationEventRowlet>(std::move(event));
    ArchiveReporter_->Enqueue(std::move(rowlet));
}

} // namespace NYT::NServer
