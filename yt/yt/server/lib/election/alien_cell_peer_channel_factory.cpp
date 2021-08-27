#include "alien_cell_peer_channel_factory.h"

#include <yt/yt/ytlib/election/alien_cell_peer_channel_factory.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NElection {

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
        TCellDirectoryPtr cellDirectory)
        : Cluster_(cluster)
        , CellId_(cellId)
        , PeerId_(peerId)
        , CellDirectory_(std::move(cellDirectory))
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

    //! Cf. IChannel::GetEndpointDescription.
    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    //! Cf. IChannel::GetEndpointAttributes.
    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TNetworkId GetNetworkId() const override
    {
        return DefaultNetworkId;
    }

    //! Returns the actual channel to be used for sending to service with
    //! a given #serviceName.
    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    //! Terminates the cached channels, if any.
    void Terminate(const TError& /*error*/) override
    { }

private:
    const TString Cluster_;
    const TCellId CellId_;
    const int PeerId_;
    const TCellDirectoryPtr CellDirectory_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    const TError UnavailableError_;

    TFuture<IChannelPtr> GetChannel()
    {
        auto address = CellDirectory_->FindPeerAddress(CellId_, PeerId_);

        if (!address) {
            return MakeFuture<IChannelPtr>(UnavailableError_);
        }

        auto channelFactory = NRpc::NBus::CreateBusChannelFactory(New<NYT::NBus::TTcpBusConfig>());
        auto channel = CreateRealmChannel(
            channelFactory->CreateChannel(*address),
            CellId_);
        return MakeFuture(channel);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TAlienCellPeerChannelFactory
    : public NElection::IAlienCellPeerChannelFactory
{
public:
    explicit TAlienCellPeerChannelFactory(TCellDirectoryPtr cellDirectory)
        : CellDirectory_(std::move(cellDirectory))
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
            CellDirectory_);
        return CreateRealmChannel(
            CreateRoamingChannel(std::move(provider)),
            cellId);
    }

private:
    const TCellDirectoryPtr CellDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

IAlienCellPeerChannelFactoryPtr CreateAlienCellPeerChannelFactory(TCellDirectoryPtr cellDirectory)
{
    return New<TAlienCellPeerChannelFactory>(
        std::move(cellDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::Election
