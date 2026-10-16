#include "yql_dq_clique_warmup_config.h"

#include "yql_dq_clique.h"

#include <util/generic/yexception.h>

namespace NYql {

TVector<TResourceManagerOptions> BuildWarmupYtBackendsFromDqConfig(const NProto::TDqConfig& config)
{
    TVector<TResourceManagerOptions> backends;
    for (const auto& backend : NormalizeYtBackendCredentials(config)) {
        TResourceManagerOptions options;
        options.YtBackend = backend;
        if (options.YtBackend.GetProxyAddress().empty()) {
            *options.YtBackend.MutableProxyAddress() = options.YtBackend.GetClusterName();
        }
        if (options.YtBackend.GetPrefix().empty()) {
            auto prefix = options.YtBackend.GetUploadPrefix();
            const auto separator = prefix.rfind('/');
            if (separator != TString::npos) {
                prefix.resize(separator);
            }
            options.YtBackend.SetPrefix(prefix);
        }
        if (options.YtBackend.GetPrefix().empty()) {
            ythrow yexception() << "YT backend " << options.YtBackend.GetClusterName()
                << " has no prefix";
        }
        if (options.YtBackend.GetUploadPrefix().empty()) {
            options.YtBackend.SetUploadPrefix(options.YtBackend.GetPrefix() + "/tmp");
        }
        backends.push_back(std::move(options));
    }
    return backends;
}

} // namespace NYql
