#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

std::vector<NApi::NNative::IConnectionPtr> CreateClusterConnections(
    const NApi::NNative::IConnectionPtr& connection,
    const TCrossClusterReplicatedStateConfigPtr& config);

std::vector<NApi::IClientBasePtr> CreateClusterClients(
    std::span<NApi::NNative::IConnectionPtr> connections,
    const NApi::NNative::TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
