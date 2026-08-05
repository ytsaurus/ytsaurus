#include "deploy_url_provider.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

TDeployStageUrlProvider& DeployStageUrlProvider()
{
    static TDeployStageUrlProvider provider;
    return provider;
}

} // namespace

void SetDeployStageUrlProvider(TDeployStageUrlProvider provider)
{
    DeployStageUrlProvider() = std::move(provider);
}

std::string GetDeployStageUrl()
{
    const auto& provider = DeployStageUrlProvider();
    return provider ? provider() : std::string{};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
