#include "node_address_provider.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

TNodeAddressProvider& NodeAddressProvider()
{
    static TNodeAddressProvider provider;
    return provider;
}

} // namespace

void SetNodeAddressProvider(TNodeAddressProvider provider)
{
    NodeAddressProvider() = std::move(provider);
}

const TNodeAddressProvider& GetNodeAddressProvider()
{
    return NodeAddressProvider();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
