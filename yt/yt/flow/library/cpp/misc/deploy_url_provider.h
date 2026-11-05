#pragma once

#include <functional>
#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Returns the link to the UI page of the deployment system stage the current node
//! runs in, or an empty string if the node is not managed by such a system.
using TDeployStageUrlProvider = std::function<std::string()>;

//! Installs the provider; called by environment-specific extensions.
void SetDeployStageUrlProvider(TDeployStageUrlProvider provider);

//! Empty when no provider is installed (e.g. in the opensource build).
std::string GetDeployStageUrl();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
