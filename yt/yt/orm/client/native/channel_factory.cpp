#include "channel_factory.h"

#include "peer_discovery.h"

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/logging/logger.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>

namespace NYT::NOrm::NClient::NNative {

using namespace NRpc::NGrpc;

////////////////////////////////////////////////////////////////////////////////

class TChannelFactory
    : public NRpc::IChannelFactory
{
public:
    TChannelFactory(
        const NRpc::NGrpc::TChannelConfigTemplatePtr& grpcChannelConfigTemplate,
        IOrmPeerDiscoveryPtr peerDiscovery,
        std::optional<TString> discoveryAddress,
        bool useIP6Addresses)
        : SerializedGrpcChannelConfigTemplate_(
            NYson::ConvertToYsonString((grpcChannelConfigTemplate)))
        , PeerDiscovery_(std::move(peerDiscovery))
        , DiscoveryAddress_(std::move(discoveryAddress))
        , UseIP6Addresses_(useIP6Addresses)
    {
        if (UseIP6Addresses_) {
            YT_VERIFY(DiscoveryAddress_.has_value());
        }
    }

private:
    const NYson::TYsonString SerializedGrpcChannelConfigTemplate_;
    const IOrmPeerDiscoveryPtr PeerDiscovery_;
    const std::optional<TString> DiscoveryAddress_;
    const bool UseIP6Addresses_;

    NRpc::IChannelPtr CreateChannel(const std::string& address) override
    {
        return CreateGrpcChannel(InstantiateConfig(address));
    }

    TChannelConfigPtr InstantiateConfig(const std::string& address) const
    {
        auto config = NYTree::ConvertTo<TChannelConfigPtr>(SerializedGrpcChannelConfigTemplate_);

        if (UseIP6Addresses_) {
            // TODO(babenko): migrate to std::string
            auto resolvedAddress = PeerDiscovery_->ResolveIP6Address(TString(address));
            YT_VERIFY(resolvedAddress);

            config->Address = *resolvedAddress;
            config->GrpcArguments[GRPC_SSL_TARGET_NAME_OVERRIDE_ARG] =
                NYTree::ConvertToNode(*DiscoveryAddress_);
        } else {
            config->Address = address;
        }
        return config;
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelFactoryPtr CreateChannelFactory(
    const NRpc::NGrpc::TChannelConfigTemplatePtr& grpcChannelConfigTemplate,
    IOrmPeerDiscoveryPtr peerDiscovery,
    std::optional<TString> discoveryAddress,
    bool useIP6Addresses)
{
    return New<TChannelFactory>(
        grpcChannelConfigTemplate,
        std::move(peerDiscovery),
        std::move(discoveryAddress),
        std::move(useIP6Addresses));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
