#include "cross_cluster_client_detail.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

namespace NYT::NCrossClusterReplicatedState {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

std::vector<NNative::IConnectionPtr> CreateClusterConnections(
    const NNative::IConnectionPtr& connection,
    const TCrossClusterReplicatedStateConfigPtr& config)
{
    auto clusterDirectory = connection->GetClusterDirectory();
    if (!clusterDirectory) {
        return {};
    }

    std::vector<NNative::IConnectionPtr> result;
    result.reserve(config->Replicas.size());

    for (const auto& replicaConfig : config->Replicas) {
        result.emplace_back(clusterDirectory->FindConnection(replicaConfig->ClusterName));
    }
    return result;
}

std::vector<IClientBasePtr> CreateClusterClients(
    std::span<NNative::IConnectionPtr> connections,
    const NNative::TClientOptions& options)
{
    std::vector<IClientBasePtr> clients;
    clients.reserve(connections.size());

    for (auto& connection : connections) {
        if (!connection) {
            clients.emplace_back();
            continue;
        }
        clients.emplace_back(connection->CreateNativeClient(options));
    }

    return clients;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
