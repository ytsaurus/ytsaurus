#pragma once

#include "public.h"

#include <yt/yt/ytlib/cell_master_client/public.h>

namespace NYT::NHiveServer::NV1 {

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NCellMasterClient::ICellDirectoryPtr masterDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NHydra::IUpstreamSynchronizerPtr upstreamSynchronizer,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV1
