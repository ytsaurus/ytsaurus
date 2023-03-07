#pragma once

#include "private.h"

#include <yt/ytlib/api/native/public.h>

#include <Interpreters/IUsersManager.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<DB::IUsersManager> CreateSecurityManager(
    TSecurityManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    TGuid cliqueId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
