#include "connection.h"

#include "config.h"

#include <yt/yt/core/rpc/config.h>

namespace NYT::NOrm::NClient::NNative::NDetail {

////////////////////////////////////////////////////////////////////////////////

NRpc::TBalancingChannelConfigPtr MakeBalancingChannelConfig(TConnectionConfigPtr config)
{
    auto configNode = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("disable_balancing_on_single_address").Value(false)
            .DoIf(config->DiscoveryAddress || !config->Addresses.empty(), [&] (auto attrs) {
                attrs.Item("addresses")
                    .BeginList()
                        .Items(NYTree::ConvertToNode(config->Addresses)->AsList())
                        .DoIf(config->DiscoveryAddress.has_value(), [&] (auto attrs) {
                            attrs.Item().Value(*config->DiscoveryAddress);
                        })
                    .EndList();
            })
            .DoIf(static_cast<bool>(config->Endpoints), [&] (auto attrs) {
                attrs.Item("endpoints").Value(NYTree::ConvertToNode(config->Endpoints)->AsMap());
            })
            .Items(NYTree::ConvertToNode(config->DynamicChannelPool)->AsMap())
        .EndMap();
    return NYTree::ConvertTo<NRpc::TBalancingChannelConfigPtr>(configNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative::NDetail
