#include <yql/tools/yqlworker/core/task_subscriptions.h>
#include <yql/tools/yqlworker/executor/common/task.h>
#include <yql/tools/yqlworker/rpc/inspector_client.h>

namespace NYql {

ITaskWatcherPtr MakeInspectorStatusPusher(
    const TString& workerId,
    const NAddr::IRemoteAddr& sourceAddr,
    const TString& taskId,
    const TString& username,
    ui32 sendUpdatesMask,
    IInspectorClient& inspectorClient,
    TTaskSubscriptionsPtr streamTaskSubscriptions,
    TTaskSubscriptionsPtr finalTaskSubscriptions);

}
