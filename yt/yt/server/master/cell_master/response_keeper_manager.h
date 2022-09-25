#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct IResponseKeeperManager
    : public virtual TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(IResponseKeeperManager)

////////////////////////////////////////////////////////////////////////////////

IResponseKeeperManagerPtr CreateResponseKeeperManager(
    TBootstrap* bootstrap,
    NHydra::IPersistentResponseKeeperPtr responseKeeper);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
