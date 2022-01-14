#include "helpers.h"

#include "dynamic_state.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/security_client/public.h>

namespace NYT::NQueueAgent {

using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NHiveClient;

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueType> DeduceQueueType(const TQueueTableRow& row)
{
    if (row.ObjectType == EObjectType::Table) {
        // NB: Dynamic and Sorted or optionals.
        if (row.Dynamic == true && row.Sorted == false) {
            return EQueueType::OrderedDynamicTable;
        }
        return TError("Only ordered dynamic tables are supported as queues");
    }

    return TError("Invalid queue object type %Qlv", row.ObjectType);
}

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr GetClusterClient(
    TClusterDirectoryPtr clusterDirectory,
    const TString& clusterName)
{
    auto client = clusterDirectory
        ->GetConnectionOrThrow(clusterName)
        ->CreateClient(NApi::TClientOptions::FromUser(QueueAgentUserName));
    auto nativeClient = MakeStrong(dynamic_cast<NApi::NNative::IClient*>(client.Get()));
    YT_VERIFY(nativeClient);
    return nativeClient;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
