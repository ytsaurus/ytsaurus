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
    , SelfClient_(SelfConnection_->CreateClient())
{ }

IConnectionPtr TClusterDirectory::GetConnection(TCellId cellId) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = CellIdMap_.find(cellId);
    return it == CellIdMap_.end() ? nullptr : it->second.Connection;
}

IConnectionPtr TClusterDirectory::GetConnectionOrThrow(TCellId cellId) const
{
    auto client = GetConnection(cellId);
    if (!client) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell id %d", ~cellId);
    }
    return client;
}

IConnectionPtr TClusterDirectory::GetConnection(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameMap_.find(clusterName);
    return it == NameMap_.end() ? nullptr : it->second.Connection;
}

IConnectionPtr TClusterDirectory::GetConnectionOrThrow(const Stroka& clusterName) const
{
    auto client = GetConnection(clusterName);
    if (!client) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %s", ~clusterName.Quote());
    }
    return client;
}

TConnectionConfigPtr TClusterDirectory::GetConnectionConfig(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameMap_.find(clusterName);
    return it == NameMap_.end() ? nullptr : it->second.ConnectionConfig;
}

std::vector<Stroka> TClusterDirectory::GetClusterNames() const
{
    TGuard<TSpinLock> guard(Lock_);
    std::vector<Stroka> result;
    for (const auto& pair : NameMap_) {
        result.push_back(pair.first);
    }
    return result;
}

void TClusterDirectory::RemoveCluster(const Stroka& clusterName)
{
    TGuard<TSpinLock> guard(Lock_);
    auto it = NameMap_.find(clusterName);
    if (it == NameMap_.end())
        return;
    auto cellId = it->second.CellId;
    NameMap_.erase(it);
    YCHECK(CellIdMap_.erase(cellId) == 1);
}

void TClusterDirectory::UpdateCluster(
    const Stroka& clusterName,
    TConnectionConfigPtr config,
    TCellId cellId)
{
    auto addNewCluster = [&] (const TCluster& cluster) {
        if (CellIdMap_.find(cluster.CellId) != CellIdMap_.end()) {
            THROW_ERROR_EXCEPTION("Duplicate cell id %d", cluster.CellId);
        }
        CellIdMap_[cluster.CellId] = cluster;
        NameMap_[cluster.Name] = cluster;
    };

    auto it = NameMap_.find(clusterName);
    if (it == NameMap_.end()) {
        auto cluster = CreateCluster(
            clusterName,
            config,
            cellId);

        TGuard<TSpinLock> guard(Lock_);
        addNewCluster(cluster);
    } else if (!AreNodesEqual(
            ConvertToNode(*(it->second.ConnectionConfig)),
            ConvertToNode(*config)))
    {
        auto cluster = CreateCluster(
            clusterName,
            config,
            cellId);

        TGuard<TSpinLock> guard(Lock_);
        CellIdMap_.erase(it->second.CellId);
        NameMap_.erase(it);
        addNewCluster(cluster);
    }
}

void TClusterDirectory::UpdateSelf()
{
    auto cluster = CreateSelfCluster();
    TGuard<TSpinLock> guard(Lock_);
    CellIdMap_[cluster.CellId] = cluster;
}

TClusterDirectory::TCluster TClusterDirectory::CreateCluster(
    const Stroka& name,
    TConnectionConfigPtr config,
    TCellId cellId) const
{
    TCluster cluster;
    cluster.Name = name;
    cluster.Connection = CreateConnection(config);
    cluster.ConnectionConfig = config;
    cluster.CellId = cellId;

    return cluster;
}

TClusterDirectory::TCluster TClusterDirectory::CreateSelfCluster() const
{
    auto resultOrError = WaitFor(SelfClient_->GetNode("//sys/@cell_id"));
    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Cannot get cell id");
    auto cellId = ConvertTo<TCellId>(resultOrError.Value());

    TCluster cluster;
    cluster.Name = "";
    cluster.Connection = SelfConnection_;
    cluster.CellId = cellId;

    return cluster;

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

