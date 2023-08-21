#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TOperationAlertEvent
{
    std::optional<NJobTrackerClient::TOperationId> OperationId;
    EOperationAlertType AlertType;
    TInstant Time;
    TError Error;
};

void Serialize(const TOperationAlertEvent& operationAlertEvent, NYson::IYsonConsumer* consumer, bool serializeOperationId = false);
void Deserialize(TOperationAlertEvent& operationAlertEvent, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
