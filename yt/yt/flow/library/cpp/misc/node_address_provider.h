#pragma once

#include <functional>
#include <optional>
#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Returns the IP address of the current node provided by the hosting
//! environment, if any, allowing to skip DNS resolution of the node's own FQDN.
using TNodeAddressProvider = std::function<std::optional<std::string>()>;

//! Installs the provider; called by environment-specific extensions.
void SetNodeAddressProvider(TNodeAddressProvider provider);

const TNodeAddressProvider& GetNodeAddressProvider();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
