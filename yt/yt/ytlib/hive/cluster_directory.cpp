#include "cluster_directory.h"

#include "private.h"

#include <yt/yt_proto/yt/client/hive/proto/cluster_directory.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NHiveClient {

using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = HiveClientLogger;

////////////////////////////////////////////////////////////////////////////////

TClusterDirectory::TClusterDirectory(NNative::TConnectionOptions connectionOptions)
    : ConnectionOptions_(std::move(connectionOptions))
{ }

NNative::IConnectionPtr TClusterDirectory::FindConnection(TCellTag cellTag) const
{
    auto guard = Guard(Lock_);
    auto it = CellTagToCluster_.find(cellTag);
    return it == CellTagToCluster_.end() ? nullptr : it->second.Connection;
}

NNative::IConnectionPtr TClusterDirectory::GetConnectionOrThrow(TCellTag cellTag) const
{
    auto connection = FindConnection(cellTag);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with tag %v", cellTag);
    }
    return connection;
}

NNative::IConnectionPtr TClusterDirectory::GetConnection(TCellTag cellTag) const
{
    auto connection = FindConnection(cellTag);
    YT_VERIFY(connection);
    return connection;
}

NNative::IConnectionPtr TClusterDirectory::FindConnection(const std::string& clusterName) const
{
    auto guard = Guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    return it == NameToCluster_.end() ? nullptr : it->second.Connection;
}

NNative::IConnectionPtr TClusterDirectory::GetConnectionOrThrow(const std::string& clusterName) const
{
    auto connection = FindConnection(clusterName);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return connection;
}

NNative::IConnectionPtr TClusterDirectory::GetConnection(const std::string& clusterName) const
{
    auto connection = FindConnection(clusterName);
    YT_VERIFY(connection);
    return connection;
}

std::vector<std::string> TClusterDirectory::GetClusterNames() const
{
    auto guard = Guard(Lock_);
    return GetKeys(NameToCluster_);
}

void TClusterDirectory::RemoveCluster(const std::string& name)
{
    auto guard = Guard(Lock_);
    auto nameIt = NameToCluster_.find(name);
    if (nameIt == NameToCluster_.end()) {
        return;
    }
    const auto& cluster = nameIt->second;
    auto cellTags = GetCellTags(cluster);
    cluster.Connection->Terminate();
    if (auto tvmId = cluster.Connection->GetConfig()->TvmId) {
        auto tvmIdsIt = ClusterTvmIds_.find(*tvmId);
        YT_VERIFY(tvmIdsIt != ClusterTvmIds_.end());
        ClusterTvmIds_.erase(tvmIdsIt);
    }
    NameToCluster_.erase(nameIt);
    for (auto cellTag : cellTags) {
        YT_VERIFY(CellTagToCluster_.erase(cellTag) == 1);
    }
    YT_LOG_DEBUG("Remote cluster unregistered (Name: %v, CellTags: %v)",
        name,
        MakeFormattableView(cellTags, TDefaultFormatter()));
}

void TClusterDirectory::Clear()
{
    auto guard = Guard(Lock_);
    CellTagToCluster_.clear();
    NameToCluster_.clear();
    ClusterTvmIds_.clear();
    YT_LOG_DEBUG("Cluster directory cleared");
}

void TClusterDirectory::UpdateCluster(const std::string& name, const INodePtr& nativeConnectionConfig)
{
    bool fire = false;
    auto addNewCluster = [&] (const TCluster& cluster) {
        for (auto cellTag : GetCellTags(cluster)) {
            if (CellTagToCluster_.contains(cellTag)) {
                THROW_ERROR_EXCEPTION("Duplicate cell tag %v", cellTag)
                    << TErrorAttribute("first_cluster_name", CellTagToCluster_[cellTag].Name)
                    << TErrorAttribute("second_cluster_name", name);
            }
            CellTagToCluster_[cellTag] = cluster;
        }
        NameToCluster_[name] = cluster;
        if (auto tvmId = cluster.Connection->GetConfig()->TvmId) {
            ClusterTvmIds_.insert(*tvmId);
        }

        fire = true;
    };

    {
        auto guard = Guard(Lock_);
        auto nameIt = NameToCluster_.find(name);
        if (nameIt == NameToCluster_.end()) {
            auto cluster = CreateCluster(name, nativeConnectionConfig);
            addNewCluster(cluster);
            auto cellTags = GetCellTags(cluster);
            YT_LOG_DEBUG("Remote cluster registered (Name: %v, CellTags: %v)",
                name,
                MakeFormattableView(cellTags, TDefaultFormatter()));
        } else if (!AreNodesEqual(nameIt->second.NativeConnectionConfig, nativeConnectionConfig)) {
            auto cluster = CreateCluster(name, nativeConnectionConfig);
            auto oldTvmId = nameIt->second.Connection->GetConfig()->TvmId;
            auto oldCellTags = GetCellTags(nameIt->second);
            nameIt->second.Connection->Terminate();
            for (auto cellTag : oldCellTags) {
                CellTagToCluster_.erase(cellTag);
            }
            NameToCluster_.erase(nameIt);
            if (oldTvmId) {
                auto tvmIdsIt = ClusterTvmIds_.find(*oldTvmId);
                YT_VERIFY(tvmIdsIt != ClusterTvmIds_.end());
                ClusterTvmIds_.erase(tvmIdsIt);
            }
            addNewCluster(cluster);
            auto cellTags = GetCellTags(cluster);
            YT_LOG_DEBUG("Remote cluster updated (Name: %v, CellTags: %v)",
                name,
                MakeFormattableView(cellTags, TDefaultFormatter()));
        }
    }

    if (fire) {
        OnClusterUpdated_.Fire(name, nativeConnectionConfig);
    }
}

void TClusterDirectory::UpdateDirectory(const NProto::TClusterDirectory& protoDirectory)
{
    THashMap<std::string, INodePtr> nameToConfig;
    for (const auto& item : protoDirectory.items()) {
        YT_VERIFY(nameToConfig.emplace(
            item.name(),
            ConvertToNode(NYson::TYsonString(item.config()))).second);
    }

    for (const auto& name : GetClusterNames()) {
        if (nameToConfig.find(name) == nameToConfig.end()) {
            RemoveCluster(name);
        }
    }

    for (const auto& [name, config] : nameToConfig) {
        UpdateCluster(name, config);
    }
}

bool TClusterDirectory::HasTvmId(NAuth::TTvmId tvmId) const
{
    auto guard = Guard(Lock_);
    return ClusterTvmIds_.find(tvmId) != ClusterTvmIds_.end();
}

TClusterDirectory::TCluster TClusterDirectory::CreateCluster(const std::string& name, const INodePtr& config)
{
    TCluster cluster{
        .Name = name,
        .NativeConnectionConfig = config,
    };

    try {
        auto typedConfig = ConvertTo<NNative::TConnectionCompoundConfigPtr>(config);
        if (!typedConfig->Static->ClusterName) {
            typedConfig->Static->ClusterName = name;
        }
        cluster.Connection = NNative::CreateConnection(typedConfig, ConnectionOptions_, MakeStrong(this));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating connection to cluster %Qv",
            name)
            << ex;
    }
    return cluster;
}

TCellTagList TClusterDirectory::GetCellTags(const TClusterDirectory::TCluster& cluster)
{
    auto secondaryTags = cluster.Connection->GetSecondaryMasterCellTags();
    // NB(coteeq): Insert primary master to the beginning for the sanity of debug messages.
    secondaryTags.insert(secondaryTags.begin(), cluster.Connection->GetPrimaryMasterCellTag());
    return secondaryTags;
}

////////////////////////////////////////////////////////////////////////////////

TClientDirectory::TClientDirectory(
    TClusterDirectoryPtr clusterDirectory,
    TClientOptions clientOptions)
    : ClusterDirectory_(std::move(clusterDirectory))
    , ClientOptions_(std::move(clientOptions))
{ }

NNative::IClientPtr TClientDirectory::FindClient(const std::string& clusterName) const
{
    const auto& connection = ClusterDirectory_->FindConnection(clusterName);
    return NNative::CreateClient(connection, ClientOptions_);
}

NNative::IClientPtr TClientDirectory::GetClientOrThrow(const std::string& clusterName) const
{
    const auto& connection = ClusterDirectory_->GetConnectionOrThrow(clusterName);
    return NNative::CreateClient(connection, ClientOptions_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
