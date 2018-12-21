#pragma once

#include <yp/server/misc/public.h>

namespace NYP::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

using TClusterTag = ui8;
using TMasterInstanceTag = ui8;

class TBootstrap;
DECLARE_REFCOUNTED_CLASS(TYTConnector)

DECLARE_REFCOUNTED_CLASS(TYTConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NMaster
