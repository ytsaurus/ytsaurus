#pragma once

#include "private.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateLocalSnapshotService(
    NElection::TCellId cellId,
    ILegacySnapshotStorePtr store,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
