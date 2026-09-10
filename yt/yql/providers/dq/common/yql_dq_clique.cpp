#include "yql_dq_clique.h"

#include <yt/yql/providers/dq/config/config.pb.h>

#include <util/folder/path.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/strip.h>

namespace NYql {

namespace {

constexpr TStringBuf CliqueValueHint =
    "pragma dq.Clique value must have format <yt_cluster>.<clique_name>: "
    "exactly one dot separating non-empty yt_cluster (without dots) and clique_name";

[[noreturn]] void ThrowInvalidClique(const TString& value) {
    throw yexception() << CliqueValueHint << ", got: \"" << value << "\"";
}

TString ResolveYtBackendProxyAddress(const NProto::TDqConfig_TYtBackend& backend) {
    if (backend.HasProxyAddress() && !backend.GetProxyAddress().empty()) {
        return backend.GetProxyAddress();
    }
    return backend.GetClusterName();
}

} // namespace

TDqCliqueRef ParseDqCliqueRef(const TString& value) {
    TStringBuf buf(value);
    const auto ytCluster = buf.NextTok('.');
    const auto cliqueName = buf.NextTok('.');
    if (!buf.empty() || ytCluster.empty() || cliqueName.empty() || value.EndsWith('.')) {
        ThrowInvalidClique(value);
    }
    return TDqCliqueRef{
        .YtCluster = TString(ytCluster),
        .CliqueName = TString(cliqueName),
    };
}

NProto::TDqConfig_TYtBackend NormalizeYtBackendCredentials(
    const NProto::TDqConfig_TYtBackend& backend)
{
    auto normalized = backend;
    if (normalized.HasToken() && !normalized.GetToken().empty()) {
        return normalized;
    }
    if (!normalized.HasTokenFile() || normalized.GetTokenFile().empty()) {
        throw yexception()
            << "Either token or token file must be specified for YT backend \""
            << backend.GetClusterName() << "\"";
    }
    TFsPath path(normalized.GetTokenFile());
    TString token = TIFStream(path).ReadAll();
    normalized.SetToken(StripString(token));
    return normalized;
}

TVector<NProto::TDqConfig_TYtBackend> NormalizeYtBackendCredentials(
    const NProto::TDqConfig& config)
{
    TVector<NProto::TDqConfig_TYtBackend> backends;
    backends.reserve(config.GetYtBackends().size());
    for (const auto& backend : config.GetYtBackends()) {
        backends.push_back(NormalizeYtBackendCredentials(backend));
    }
    return backends;
}

bool IsCommunalYtBackend(const NProto::TDqConfig_TYtBackend& backend) {
    return backend.GetMaxJobs() > 0;
}

TVector<NProto::TDqConfig_TYtBackend> FilterCommunalYtBackends(
    const TVector<NProto::TDqConfig_TYtBackend>& backends)
{
    TVector<NProto::TDqConfig_TYtBackend> communal;
    for (const auto& backend : backends) {
        if (IsCommunalYtBackend(backend)) {
            communal.push_back(backend);
        }
    }
    return communal;
}

TDqYtClusterResolver MakeYtBackendResolver(
    TVector<NProto::TDqConfig_TYtBackend> backends)
{
    return [backends = std::move(backends)] (const TString& clusterShortcut) -> TMaybe<TDqYtClusterBinding> {
        for (const auto& backend : backends) {
            if (backend.GetClusterName() == clusterShortcut) {
                return TDqYtClusterBinding{
                    .ClusterName = backend.GetClusterName(),
                    .ProxyAddress = ResolveYtBackendProxyAddress(backend),
                    .Token = backend.GetToken(),
                };
            }
        }
        return Nothing();
    };
}

TDqYtClusterBinding ResolveYtClusterBindingOrThrow(
    const TDqYtClusterResolver& resolveYtCluster,
    const TString& ytClusterShortcut)
{
    const auto ytBinding = resolveYtCluster(ytClusterShortcut);
    if (!ytBinding) {
        throw yexception()
            << "Unknown YT backend \"" << ytClusterShortcut
            << "\" from pragma dq.Clique; check dq YtBackends ClusterName";
    }
    if (ytBinding->ProxyAddress.empty()) {
        throw yexception()
            << "YT backend \"" << ytClusterShortcut << "\" has empty ProxyAddress";
    }
    if (ytBinding->Token.empty()) {
        throw yexception()
            << "YT backend \"" << ytClusterShortcut << "\" has empty token";
    }
    return *ytBinding;
}

void ValidateDqCliqueYtBackend(
    const TVector<NProto::TDqConfig_TYtBackend>& backends,
    const TString& cliqueValue)
{
    const auto cliqueRef = ParseDqCliqueRef(cliqueValue);
    const auto resolveYtCluster = MakeYtBackendResolver(backends);
    ResolveYtClusterBindingOrThrow(resolveYtCluster, cliqueRef.YtCluster);
}

} // namespace NYql
