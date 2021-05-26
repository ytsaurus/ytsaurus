#include "alien_cell_peer_channel_factory.h"

#include <yt/yt/ytlib/election/alien_cell_peer_channel_factory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NElection {

using namespace NApi;
using namespace NRpc;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// TODO(savrus) This is a proof of concept channel to make simple test work.
// Future development need to consider:
// 1) Maintain at masters alien cell directory and use it to find peers
// 2) Wrap channel in CreateFailureDetectingChannel and refresh peer address on failure.
// 3) Maybe cache channel as TSchedulerChannelProvider does

class TAlienCellPeerChannelProvider
    : public IRoamingChannelProvider
{
public:
    TAlienCellPeerChannelProvider(
        TClusterDirectoryPtr clusterDirectory,
        TClusterDirectorySynchronizerPtr clusterDirectorySynchronizer,
        const TString& cluster,
        TCellId cellId,
        int peerId)
        : ClusterDirectory_(std::move(clusterDirectory))
        , ClusterDirectorySynchronizer_(std::move(clusterDirectorySynchronizer))
        , Cluster_(cluster)
        , CellId_(cellId)
        , PeerId_(peerId)
        , EndpointDescription_(Format("%v:%v:%v",
            Cluster_,
            CellId_,
            PeerId_))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("cluster").Value(Cluster_)
                .Item("cell_id").Value(CellId_)
                .Item("peer_id").Value(PeerId_)
            .EndMap()))
    { }

    //! Cf. IChannel::GetEndpointDescription.
    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    //! Cf. IChannel::GetEndpointAttributes.
    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TNetworkId GetNetworkId() const override
    {
        return DefaultNetworkId;
    }

    //! Returns the actual channel to be used for sending to service with
    //! a given #serviceName.
    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    //! Terminates the cached channels, if any.
    virtual void Terminate(const TError& /*error*/) override
    { }

private:
    const TClusterDirectoryPtr ClusterDirectory_;
    const TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer_;
    const TString Cluster_;
    const TCellId CellId_;
    const int PeerId_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    TFuture<IChannelPtr> GetChannel()
    {
        return GetConnection()
            .Apply(BIND(&TAlienCellPeerChannelProvider::OnConnect, MakeStrong(this)));
    }

    TErrorOr<IChannelPtr> OnConnect(const TErrorOr<IConnectionPtr>& connectionOrError)
    {
        if (!connectionOrError.IsOK()) {
            TErrorOr<IChannelPtr> result;
            result.SetCode(connectionOrError.GetCode());
            result.SetMessage(connectionOrError.GetMessage());
            *result.MutableInnerErrors() = connectionOrError.InnerErrors();
            return result;
        }

        auto connection = connectionOrError.Value();
        try {
            return GetCellChannel(connection);
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }

    IChannelPtr GetCellChannel(IConnectionPtr connection)
    {
        auto path = Format("%v/@peers/%v/address", TYPath(FromObjectId(CellId_)), PeerId_);
        auto options = TClientOptions::FromUser(RootUserName);
        auto client = connection->CreateClient(options);
        auto ysonAddress = WaitFor(client->GetNode(path))
            .ValueOrThrow();
        auto address = NYTree::ConvertTo<TString>(ysonAddress);
        auto channelFactory = NRpc::NBus::CreateBusChannelFactory(New<NYT::NBus::TTcpBusConfig>());
        return CreateRealmChannel(
            channelFactory->CreateChannel(address),
            CellId_);
    }

    TFuture<IConnectionPtr> GetConnection()
    {
        if (auto connection = GetClusterConnection(); connection) {
            return MakeFuture(connection);
        }
        if (!ClusterDirectorySynchronizer_) {
            return MakeNoConnectionError();
        }
        return ClusterDirectorySynchronizer_->Sync().Apply(BIND([=, this_ = MakeStrong(this)] {
            auto connection = GetClusterConnection();
            if (connection) {
                return MakeFuture(connection);
            }
            return MakeNoConnectionError();
        }));
    }

    IConnectionPtr GetClusterConnection()
    {
        return ClusterDirectory_->FindConnection(Cluster_);
    }

    TFuture<IConnectionPtr> MakeNoConnectionError()
    {
        return MakeFuture<IConnectionPtr>(TError(
            NRpc::EErrorCode::Unavailable,
            "No such cluster %Qv",
            Cluster_));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAlienCellPeerChannelFactory
    : public NElection::IAlienCellPeerChannelFactory
{
public:
    TAlienCellPeerChannelFactory(
        TClusterDirectoryPtr clusterDirectory,
        TClusterDirectorySynchronizerPtr clusterDirectorySynchronizer)
        : ClusterDirectory_(std::move(clusterDirectory))
        , ClusterDirectorySynchronizer_(std::move(clusterDirectorySynchronizer))
    { }

    virtual NRpc::IChannelPtr CreateChannel(const TString& cluster, TCellId cellId, int peerId) override
    {
        auto provider = New<TAlienCellPeerChannelProvider>(
            ClusterDirectory_,
            ClusterDirectorySynchronizer_,
            cluster,
            cellId,
            peerId);
        return CreateRealmChannel(
            CreateRoamingChannel(std::move(provider)),
            cellId);
    }

private:
    const TClusterDirectoryPtr ClusterDirectory_;
    const TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer_;
};

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(
    TClusterDirectoryPtr clusterDirectory,
    TClusterDirectorySynchronizerPtr clusterDirectorySynchronizer)
{
    return New<TAlienCellPeerChannelFactory>(
        std::move(clusterDirectory),
        std::move(clusterDirectorySynchronizer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election
