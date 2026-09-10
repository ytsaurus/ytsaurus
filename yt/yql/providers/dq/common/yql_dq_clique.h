#pragma once

#include <functional>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NProto {
class TDqConfig;
class TDqConfig_TYtBackend;
} // namespace NYql::NProto

namespace NYql {

struct TDqCliqueRef {
    TString YtCluster;
    TString CliqueName;
};

struct TDqYtClusterBinding {
    TString ClusterName;
    TString ProxyAddress;
    TString Token;
};

using TDqYtClusterResolver = std::function<TMaybe<TDqYtClusterBinding>(const TString& ytClusterShortcut)>;

TDqCliqueRef ParseDqCliqueRef(const TString& value);

NProto::TDqConfig_TYtBackend NormalizeYtBackendCredentials(
    const NProto::TDqConfig_TYtBackend& backend);

TVector<NProto::TDqConfig_TYtBackend> NormalizeYtBackendCredentials(
    const NProto::TDqConfig& config);

bool IsCommunalYtBackend(const NProto::TDqConfig_TYtBackend& backend);

TVector<NProto::TDqConfig_TYtBackend> FilterCommunalYtBackends(
    const TVector<NProto::TDqConfig_TYtBackend>& backends);

TDqYtClusterResolver MakeYtBackendResolver(
    TVector<NProto::TDqConfig_TYtBackend> backends);

TDqYtClusterBinding ResolveYtClusterBindingOrThrow(
    const TDqYtClusterResolver& resolveYtCluster,
    const TString& ytClusterShortcut);

// Validates pragma dq.Clique value: format and YT backend ClusterName (no network I/O).
void ValidateDqCliqueYtBackend(
    const TVector<NProto::TDqConfig_TYtBackend>& backends,
    const TString& cliqueValue);

} // namespace NYql
