#include "helpers.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NMaintenanceTrackerServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SerializeMaintenanceRequestsOf(
    const TNontemplateMaintenanceTargetBase* maintenanceTarget,
    IYsonConsumer* consumer)
{
    TCompactVector<std::pair<TMaintenanceId, TMaintenanceRequest>, TypicalMaintenanceRequestCount> requests(
        maintenanceTarget->MaintenanceRequests().begin(),
        maintenanceTarget->MaintenanceRequests().end());
    Sort(requests, [] (const auto& lhs, const auto& rhs) {
        return lhs.second.Type < rhs.second.Type;
    });
    BuildYsonFluently(consumer)
        .DoMapFor(requests, [] (TFluentMap map, const auto& request) {
            map.Item(ToString(request.first))
                .BeginMap()
                    .Item("user").Value(request.second.User)
                    .Item("comment").Value(request.second.Comment)
                    .Item("timestamp").Value(request.second.Timestamp)
                    .Item("type").Value(request.second.Type)
                .EndMap();
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
