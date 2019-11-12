#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

NCluster::IClusterReaderPtr CreateClusterReader(
    TClusterReaderConfigPtr config,
    NClient::NApi::NNative::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
