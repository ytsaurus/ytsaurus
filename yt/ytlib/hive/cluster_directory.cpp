#include "cluster_directory.h"

#include <core/ytree/convert.h>

#include <core/concurrency/fiber.h>

#include <ytlib/api/config.h>
#include <ytlib/api/connection.h>
#include <ytlib/api/client.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TClusterDirectory::TClusterDirectory(IConnectionPtr selfConnection)
    : SelfConnection_(selfConnection)
    , SelfClient_(SelfConnection_->CreateClient(GetRootClientOptions()))
{ }

IConnectionPtr TClusterDirectory::GetConnection(TCellTag cellTag) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = CellTagToCluster_.find(cellTag);
    return it == CellTagToCluster_.end() ? nullptr : it->second.Connection;
}

IConnectionPtr TClusterDirectory::GetConnectionOrThrow(TCellTag cellTag) const
{
    auto connection = GetConnection(cellTag);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell tag %v", cellTag);
    }
    return connection;
}

IConnectionPtr TClusterDirectory::GetConnection(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    return it == NameToCluster_.end() ? nullptr : it->second.Connection;
}

IConnectionPtr TClusterDirectory::GetConnectionOrThrow(const Stroka& clusterName) const
{
    auto connection = GetConnection(clusterName);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return connection;
}

TNullable<Stroka> TClusterDirectory::GetDefaultNetwork(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    if (it == NameToCluster_.end()) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return it == NameToCluster_.end() ? Null : it->second.DefaultNetwork;
}

TConnectionConfigPtr TClusterDirectory::GetConnectionConfig(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    return it == NameToCluster_.end() ? nullptr : it->second.ConnectionConfig;
}

TConnectionConfigPtr TClusterDirectory::GetConnectionConfigOrThrow(const Stroka& clusterName) const
{
    auto connectionConfig = GetConnectionConfig(clusterName);
    if (!connectionConfig) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return connectionConfig;
}

std::vector<Stroka> TClusterDirectory::GetClusterNames() const
{
    TGuard<TSpinLock> guard(Lock_);
    return GetKeys(NameToCluster_);
}

void TClusterDirectory::RemoveCluster(const Stroka& clusterName)
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    if (it == NameToCluster_.end())
        return;
    auto cellTag = it->second.CellTag;
    NameToCluster_.erase(it);
    YCHECK(CellTagToCluster_.erase(cellTag) == 1);
}

void TClusterDirectory::UpdateCluster(
    const Stroka& clusterName,
    TConnectionConfigPtr config,
    TCellTag cellTag,
    const TNullable<Stroka>& defaultNetwork)
{
    auto addNewCluster = [&] (const TCluster& cluster) {
        if (CellTagToCluster_.find(cluster.CellTag) != CellTagToCluster_.end()) {
            THROW_ERROR_EXCEPTION("Duplicate cell id %v", cluster.CellTag);
        }
        CellTagToCluster_[cluster.CellTag] = cluster;
        NameToCluster_[cluster.Name] = cluster;
    };

    auto it = NameToCluster_.find(clusterName);
    if (it == NameToCluster_.end()) {
        auto cluster = CreateCluster(
            clusterName,
            config,
            cellTag,
            defaultNetwork);

        TGuard<TSpinLock> guard(Lock_);
        addNewCluster(cluster);
    } else if (!AreNodesEqual(
            ConvertToNode(*(it->second.ConnectionConfig)),
            ConvertToNode(*config)) ||
            !(defaultNetwork == it->second.DefaultNetwork))
    {
        auto cluster = CreateCluster(
            clusterName,
            config,
            cellTag,
            defaultNetwork);

        TGuard<TSpinLock> guard(Lock_);
        CellTagToCluster_.erase(it->second.CellTag);
        NameToCluster_.erase(it);
        addNewCluster(cluster);
    }
}

void TClusterDirectory::UpdateSelf()
{
    auto cluster = CreateSelfCluster();
    TGuard<TSpinLock> guard(Lock_);
    CellTagToCluster_[cluster.CellTag] = cluster;
}

TClusterDirectory::TCluster TClusterDirectory::CreateCluster(
    const Stroka& name,
    TConnectionConfigPtr config,
    TCellTag cellTag,
    TNullable<Stroka> defaultNetwork) const
{
    TCluster cluster;
    cluster.Name = name;
    cluster.Connection = CreateConnection(config);
    cluster.ConnectionConfig = config;
    cluster.CellTag = cellTag;
    cluster.DefaultNetwork = defaultNetwork;

    return cluster;
}

TClusterDirectory::TCluster TClusterDirectory::CreateSelfCluster() const
{
    auto resultOrError = WaitFor(SelfClient_->GetNode("//sys/@cell_tag"));
    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error getting cluster cell tag");
    auto cellTag = ConvertTo<TCellTag>(resultOrError.Value());

    TCluster cluster;
    cluster.Name = "";
    cluster.Connection = SelfConnection_;
    cluster.CellTag = cellTag;
    cluster.DefaultNetwork = Null;

    return cluster;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

