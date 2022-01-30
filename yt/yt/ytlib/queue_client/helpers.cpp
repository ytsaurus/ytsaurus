#include "helpers.h"

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/core/ypath/token.h>

namespace NYT::NQueueClient {

using namespace NOrchid;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TYPath GetQueueRemotePath(const TString& cluster, const TYPath& queuePath)
{
    auto queueRef = Format("%v:%v", cluster, queuePath);
    return Format("//queue_agent/queues/%v", ToYPathLiteral(queueRef));
}

IYPathServicePtr CreateQueueYPathService(
    IChannelPtr queueAgentChannel,
    const TString& cluster,
    const TYPath& queuePath)
{
    return CreateOrchidYPathService(TOrchidOptions{
        .Channel = std::move(queueAgentChannel),
        .RemoteRoot = GetQueueRemotePath(cluster, queuePath),
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
