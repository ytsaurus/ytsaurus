#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellarManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellarConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCellarManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCellarConfig)
DECLARE_REFCOUNTED_CLASS(TCellarOccupantConfig)

DECLARE_REFCOUNTED_STRUCT(ICellar)
DECLARE_REFCOUNTED_STRUCT(ICellarManager)
DECLARE_REFCOUNTED_STRUCT(ICellarOccupant)

DECLARE_REFCOUNTED_STRUCT(ICellarOccupier)
DECLARE_REFCOUNTED_STRUCT(ICellarOccupierProvider)

DECLARE_REFCOUNTED_STRUCT(ICellarBootstrapProxy)

DEFINE_ENUM(ECellarType,
    ((Tablet)          (0))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
