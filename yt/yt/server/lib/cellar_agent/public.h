#pragma once

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/client/election/public.h>

#include <yt/yt/core/misc/common.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellarManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellarConfig)
DECLARE_REFCOUNTED_CLASS(TCellarManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCellarDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCellarOccupantConfig)

DECLARE_REFCOUNTED_STRUCT(ICellar)
DECLARE_REFCOUNTED_STRUCT(ICellarManager)
DECLARE_REFCOUNTED_STRUCT(ICellarOccupant)

DECLARE_REFCOUNTED_STRUCT(ICellarOccupier)
DECLARE_REFCOUNTED_STRUCT(ICellarOccupierProvider)

DECLARE_REFCOUNTED_STRUCT(ICellarBootstrapProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
