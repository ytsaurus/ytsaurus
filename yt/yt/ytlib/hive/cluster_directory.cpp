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

NNative::IConnectionPtr TClusterDirectory::CreateConnection(
    const std::string& name,
    const NYTree::INodePtr& config)
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
