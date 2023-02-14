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

static const auto& Logger = HiveClientLogger;

////////////////////////////////////////////////////////////////////////////////

TClusterDirectory::TClusterDirectory(NNative::TConnectionOptions connectionOptions)
    : ConnectionOptions_(std::move(connectionOptions))
{ }

NNative::IConnectionPtr TClusterDirectory::FindConnection(TClusterTag clusterTag) const
{
    auto guard = Guard(Lock_);
    auto it = ClusterTagToCluster_.find(clusterTag);
    return it == ClusterTagToCluster_.end() ? nullptr : it->second.Connection;
}

NNative::IConnectionPtr TClusterDirectory::GetConnectionOrThrow(TClusterTag clusterTag) const
{
    auto connection = FindConnection(clusterTag);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with tag %v", clusterTag);
    }
    return connection;
}

NNative::IConnectionPtr TClusterDirectory::FindConnection(const TString& clusterName) const
{
    auto guard = Guard(Lock_);
    auto it = NameToCluster_.find(clusterName);
    return it == NameToCluster_.end() ? nullptr : it->second.Connection;
}

NNative::IConnectionPtr TClusterDirectory::GetConnectionOrThrow(const TString& clusterName) const
{
    auto connection = FindConnection(clusterName);
    if (!connection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %Qv", clusterName);
    }
    return connection;
}

std::vector<TString> TClusterDirectory::GetClusterNames() const
{
    auto guard = Guard(Lock_);
    return GetKeys(NameToCluster_);
}

void TClusterDirectory::RemoveCluster(const TString& name)
{
    auto guard = Guard(Lock_);
    auto nameIt = NameToCluster_.find(name);
    if (nameIt == NameToCluster_.end()) {
        return;
    }
    const auto& cluster = nameIt->second;
    auto clusterTag = GetClusterTag(cluster);
    cluster.Connection->Terminate();
    if (auto tvmId = cluster.Connection->GetConfig()->TvmId) {
        auto tvmIdsIt = ClusterTvmIds_.find(*tvmId);
        YT_VERIFY(tvmIdsIt != ClusterTvmIds_.end());
        ClusterTvmIds_.erase(tvmIdsIt);
    }
    NameToCluster_.erase(nameIt);
    YT_VERIFY(ClusterTagToCluster_.erase(clusterTag) == 1);
    YT_LOG_DEBUG("Remote cluster unregistered (Name: %v, ClusterTag: %v)",
        name,
        clusterTag);
}

void TClusterDirectory::Clear()
{
    auto guard = Guard(Lock_);
    ClusterTagToCluster_.clear();
    NameToCluster_.clear();
    ClusterTvmIds_.clear();
}

void TClusterDirectory::UpdateCluster(const TString& name, INodePtr nativeConnectionConfig)
{
    auto addNewCluster = [&] (const TCluster& cluster) {
        auto clusterTag = GetClusterTag(cluster);
        if (ClusterTagToCluster_.find(clusterTag) != ClusterTagToCluster_.end()) {
            THROW_ERROR_EXCEPTION("Duplicate cluster tag %v", clusterTag);
        }
        ClusterTagToCluster_[clusterTag] = cluster;
        NameToCluster_[name] = cluster;
        if (auto tvmId = cluster.Connection->GetConfig()->TvmId) {
            ClusterTvmIds_.insert(*tvmId);
        }
    };

    auto nameIt = NameToCluster_.find(name);
    if (nameIt == NameToCluster_.end()) {
        auto cluster = CreateCluster(name, nativeConnectionConfig);
        auto guard = Guard(Lock_);
        addNewCluster(cluster);
        YT_LOG_DEBUG("Remote cluster registered (Name: %v, ClusterTag: %v)",
            name,
            cluster.Connection->GetClusterTag());
    } else if (!AreNodesEqual(nameIt->second.NativeConnectionConfig, nativeConnectionConfig)) {
        auto cluster = CreateCluster(name, nativeConnectionConfig);
        auto guard = Guard(Lock_);
        auto oldTvmId = nameIt->second.Connection->GetConfig()->TvmId;
        nameIt->second.Connection->Terminate();
        ClusterTagToCluster_.erase(GetClusterTag(nameIt->second));
        NameToCluster_.erase(nameIt);
        if (oldTvmId) {
            auto tvmIdsIt = ClusterTvmIds_.find(*oldTvmId);
            YT_VERIFY(tvmIdsIt != ClusterTvmIds_.end());
            ClusterTvmIds_.erase(tvmIdsIt);
        }
        addNewCluster(cluster);
        YT_LOG_DEBUG("Remote cluster updated (Name: %v, ClusterTag: %v)",
            name,
            cluster.Connection->GetClusterTag());
    }

    OnClusterUpdated_.Fire(name, nativeConnectionConfig);
}

void TClusterDirectory::UpdateDirectory(const NProto::TClusterDirectory& protoDirectory)
{
    THashMap<TString, INodePtr> nameToConfig;
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

TClusterDirectory::TCluster TClusterDirectory::CreateCluster(const TString& name, INodePtr config) const
{
    TCluster cluster;
    cluster.NativeConnectionConfig = config;
    try {
        auto typedConfig = ConvertTo<NNative::TConnectionCompoundConfigPtr>(config);
        if (!typedConfig->Static->ClusterName) {
            typedConfig->Static->ClusterName = name;
        }
        cluster.Connection = NNative::CreateConnection(typedConfig, ConnectionOptions_);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating connection to cluster %Qv",
            name)
            << ex;
    }
    return cluster;
}

TClusterTag TClusterDirectory::GetClusterTag(const TClusterDirectory::TCluster& cluster)
{
    return cluster.Connection->GetClusterTag();
}

////////////////////////////////////////////////////////////////////////////////

TClientDirectory::TClientDirectory(
    TClusterDirectoryPtr clusterDirectory,
    TClientOptions clientOptions)
    : ClusterDirectory_(std::move(clusterDirectory))
    , ClientOptions_(std::move(clientOptions))
{ }

NNative::IClientPtr TClientDirectory::FindClient(const TString& clusterName) const
{
    const auto& connection = ClusterDirectory_->FindConnection(clusterName);
    return NNative::CreateClient(connection, ClientOptions_);
}

NNative::IClientPtr TClientDirectory::GetClientOrThrow(const TString& clusterName) const
{
    const auto& connection = ClusterDirectory_->GetConnectionOrThrow(clusterName);
    return NNative::CreateClient(connection, ClientOptions_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
