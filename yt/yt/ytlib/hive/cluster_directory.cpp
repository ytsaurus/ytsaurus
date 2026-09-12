#include "cluster_directory.h"

#include "private.h"

#include <yt/yt_proto/yt/client/hive/proto/cluster_directory.pb.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/composite_map.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NHiveClient {

using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NYTree;
using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectory::TClustersOrchid
    : public TVirtualMapBase
{
public:
    explicit TClustersOrchid(TClusterDirectoryPtr clusterDirectory)
        : ClusterDirectory_(std::move(clusterDirectory))
    { }

private:
    const TClusterDirectoryPtr ClusterDirectory_;

    std::vector<std::string> GetKeys(i64 limit) const override
    {
        auto keys = ClusterDirectory_->GetClusterNames();
        if (std::ssize(keys) > limit) {
            keys.resize(limit);
        }
        return keys;
    }

    i64 GetSize() const override
    {
        return std::ssize(ClusterDirectory_->GetClusterNames());
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        auto cluster = ClusterDirectory_->FindCluster(key);
        if (!cluster) {
            return nullptr;
        }

        return IYPathService::FromProducer(BIND([cluster = std::move(*cluster)] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("static_config").Value(cluster.Connection->GetStaticConfig())
                    .Item("dynamic_config").Value(cluster.Connection->GetConfig())
                    .Item("config_layers")
                        .BeginMap()
                            .Item("cluster_directory").Value(cluster.ConnectionConfig)
                            // TODO(ifsmirnov): YT-29431: support dynamic reconfiguration.
                            .Item("dynamic_config_patch")
                                .BeginMap()
                                .EndMap()
                        .EndMap()
                .EndMap();
        }));
    }
};

IYPathServicePtr TClusterDirectory::GetOrchidService()
{
    // TODO(ifsmirnov): YT-29431: support dynamic reconfiguration.
    auto dynamicConfigPatches = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
    }));

    return CreateCompositeMapService()
        ->AddChild("clusters", New<TClustersOrchid>(MakeStrong(this)))
        ->AddChild("dynamic_config_patches", std::move(dynamicConfigPatches));
}

NNative::IConnectionPtr TClusterDirectory::CreateConnection(
    const std::string& name,
    const INodePtr& config)
{
    auto typedConfig = ConvertTo<NNative::TConnectionCompoundConfigPtr>(config);
    if (!typedConfig->Static->ClusterName) {
        typedConfig->Static->ClusterName = name;
    }
    return NNative::CreateConnection(typedConfig, ConnectionOptions_, MakeStrong(this));
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
