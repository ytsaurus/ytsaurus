#include "yql_dq_clique_warmup_config.h"

#include "yql_dq_clique.h"

namespace NYql {

TVector<TResourceManagerOptions> BuildWarmupYtBackendsFromDqConfig(const NProto::TDqConfig& config)
{
    TVector<TResourceManagerOptions> backends;
    for (const auto& backend : FilterCommunalYtBackends(NormalizeYtBackendCredentials(config))) {
        TResourceManagerOptions options;
        options.YtBackend = backend;
        if (options.YtBackend.GetProxyAddress().empty()) {
            *options.YtBackend.MutableProxyAddress() = options.YtBackend.GetClusterName();
        }
        backends.push_back(std::move(options));
    }
    return backends;
}

} // namespace NYql
