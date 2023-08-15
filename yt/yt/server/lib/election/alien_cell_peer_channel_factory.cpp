#include "alien_cell_peer_channel_factory.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/native_authenticating_channel.h>

#include <yt/yt/ytlib/election/alien_cell_peer_channel_factory.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NElection {

using namespace NAuth;
using namespace NRpc;
using namespace NHiveClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TAlienCellPeerChannelProvider
    : public IRoamingChannelProvider
{
public:
    TAlienCellPeerChannelProvider(
        const TString& cluster,
        TCellId cellId,
        int peerId,
        ICellDirectoryPtr cellDirectory,
        TClusterDirectoryPtr clusterDirectory,
        ITvmServicePtr tvmService)
        : Cluster_(cluster)
        , CellId_(cellId)
        , PeerId_(peerId)
        , CellDirectory_(std::move(cellDirectory))
        , ClusterDirectory_(std::move(clusterDirectory))
        , TvmService_(std::move(tvmService))
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
        , UnavailableError_(TError(NRpc::EErrorCode::Unavailable, "Peer is not available")
            << TErrorAttribute("endpoint", EndpointDescription_))
    { }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        auto address = CellDirectory_->FindPeerAddress(CellId_, PeerId_);

        if (!address) {
            return MakeFuture<IChannelPtr>(UnavailableError_);
        }

        auto channelFactory = NRpc::NBus::CreateTcpBusChannelFactory(New<NYT::NBus::TBusConfig>());
        auto channel = CreateRealmChannel(
            channelFactory->CreateChannel(*address),
            CellId_);

        auto connection = ClusterDirectory_->FindConnection(Cluster_);
        if (!connection) {
            return MakeFuture<IChannelPtr>(
                    TError(NRpc::EErrorCode::Unavailable, "Cannot find such cluster")
                        << TErrorAttribute("cluster", Cluster_));
        }
        auto clusterConfig = connection->GetConfig();

        channel = NAuth::CreateNativeAuthenticationInjectingChannel(
            std::move(channel),
            clusterConfig->TvmId);

        return MakeFuture(std::move(channel));
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    void Terminate(const TError& /*error*/) override
    { }

private:
    const TString Cluster_;
    const TCellId CellId_;
    const int PeerId_;
    const ICellDirectoryPtr CellDirectory_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const ITvmServicePtr TvmService_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    const TError UnavailableError_;
};

////////////////////////////////////////////////////////////////////////////////

class TAlienCellPeerChannelFactory
    : public NElection::IAlienCellPeerChannelFactory
{
public:
    TAlienCellPeerChannelFactory(
        ICellDirectoryPtr cellDirectory,
        TClusterDirectoryPtr clusterDirectory)
        : CellDirectory_(std::move(cellDirectory))
        , ClusterDirectory_(std::move(clusterDirectory))
        , TvmService_(TNativeAuthenticationManager::Get()->GetTvmService())
    { }

    NRpc::IChannelPtr CreateChannel(
        const TString& cluster,
        TCellId cellId,
        int peerId) override
    {
        auto provider = New<TAlienCellPeerChannelProvider>(
            cluster,
            cellId,
            peerId,
            CellDirectory_,
            ClusterDirectory_,
            TvmService_);

        return CreateRealmChannel(
            CreateRoamingChannel(std::move(provider)),
            cellId);
    }

private:
    const ICellDirectoryPtr CellDirectory_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const ITvmServicePtr TvmService_;
};

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(
    ICellDirectoryPtr cellDirectory,
    TClusterDirectoryPtr clusterDirectory)
{
    return New<TAlienCellPeerChannelFactory>(
        std::move(cellDirectory),
        std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election
