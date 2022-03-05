#include "helpers.h"

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/core/ypath/token.h>

namespace NYT::NQueueClient {

using namespace NOrchid;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TYPath GetQueueAgentObjectRemotePath(const TString& cluster, const TString& objectKind, const TYPath& objectPath)
{
    auto objectRef = Format("%v:%v", cluster, objectPath);
    // NB: Mind the plural!
    return Format("//queue_agent/%vs/%v", objectKind, ToYPathLiteral(objectRef));
}

IYPathServicePtr CreateQueueAgentYPathService(
    IChannelPtr queueAgentChannel,
    const TString& cluster,
    const TString& objectKind,
    const TYPath& objectPath)
{
    return CreateOrchidYPathService(TOrchidOptions{
        .Channel = std::move(queueAgentChannel),
        .RemoteRoot = GetQueueAgentObjectRemotePath(cluster, objectKind, objectPath),
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
