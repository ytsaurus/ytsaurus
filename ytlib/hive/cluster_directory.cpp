#include "public.h"
#include "cluster_directory.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/hive/cluster_directory.pb.h>

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NHiveClient {

using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

INativeConnectionPtr TClusterDirectory::FindConnection(TCellTag cellTag) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = CellTagToCluster_.find(cellTag);
    return it == CellTagToCluster_.end() ? nullptr : it->second.Connection;
}

INativeConnectionPtr TClusterDirectory::GetConnectionOrThrow(TCellTag cellTag) const
{
    auto connection = FindConnection(cellTag);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell tag %v", cellTag);
    }
    return connection;
}

INativeConnectionPtr TClusterDirectory::FindConnection(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    return it == NameToCluster_.end() ? nullptr : it->second.Connection;
}

INativeConnectionPtr TClusterDirectory::GetConnectionOrThrow(const Stroka& clusterName) const
{
    auto connection = FindConnection(clusterName);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return connection;
}

std::vector<Stroka> TClusterDirectory::GetClusterNames() const
{
    TGuard<TSpinLock> guard(Lock_);
    return GetKeys(NameToCluster_);
}

void TClusterDirectory::RemoveCluster(const Stroka& name)
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameToCluster_.find(name);
    if (it == NameToCluster_.end()) {
        return;
    }
    auto cellTag = GetCellTag(it->second);
    NameToCluster_.erase(it);
    YCHECK(CellTagToCluster_.erase(cellTag) == 1);
}

void TClusterDirectory::UpdateCluster(const Stroka& name, INodePtr config)
{
    auto addNewCluster = [&] (const TCluster& cluster) {
        auto cellTag = GetCellTag(cluster);
        if (CellTagToCluster_.find(cellTag) != CellTagToCluster_.end()) {
            THROW_ERROR_EXCEPTION("Duplicate cell tag %v", cellTag);
        }
        CellTagToCluster_[cellTag] = cluster;
        NameToCluster_[name] = cluster;
    };

    auto it = NameToCluster_.find(name);
    if (it == NameToCluster_.end()) {
        auto cluster = CreateCluster(name, config);
        TGuard<TSpinLock> guard(Lock_);
        addNewCluster(cluster);
    } else if (!AreNodesEqual(it->second.Config, config)) {
        auto cluster = CreateCluster(name, config);
        TGuard<TSpinLock> guard(Lock_);
        CellTagToCluster_.erase(GetCellTag(it->second));
        NameToCluster_.erase(it);
        addNewCluster(cluster);
    }
}

void TClusterDirectory::UpdateDirectory(const NProto::TClusterDirectory& protoDirectory)
{
    yhash_map<Stroka, INodePtr> nameToConfig;
    for (const auto& item : protoDirectory.items()) {
        YCHECK(nameToConfig.emplace(
            item.name(),
            ConvertToNode(NYson::TYsonString(item.config()))).second);
    }

    for (const auto& name : GetClusterNames()) {
        if (nameToConfig.find(name) == nameToConfig.end()) {
            RemoveCluster(name);
        }
    }

    for (const auto& pair : nameToConfig) {
        UpdateCluster(pair.first, pair.second);
    }
}

TClusterDirectory::TCluster TClusterDirectory::CreateCluster(const Stroka& name, INodePtr config) const
{
    TCluster cluster;
    cluster.Config = config;
    try {
        // TODO(babenko): no native connection here
        cluster.Connection = dynamic_cast<INativeConnection*>(CreateConnection(config).Get());
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating connection to cluster %Qv",
            name)
            << ex;
    }
    return cluster;
}

TCellTag TClusterDirectory::GetCellTag(const TClusterDirectory::TCluster& cluster)
{
    return cluster.Connection->GetCellTag();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT

