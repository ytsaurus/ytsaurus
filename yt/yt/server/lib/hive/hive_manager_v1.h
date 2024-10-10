#pragma once

#include "public.h"

namespace NYT::NHiveServer::NV1 {

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NHydra::IUpstreamSynchronizerPtr upstreamSynchronizer,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV1
