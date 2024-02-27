#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/client/election/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

NCellarClient::ECellarType GetCellarTypeFromCellId(NElection::TCellId id);
NCellarClient::ECellarType GetCellarTypeFromCellBundleId(NObjectClient::TObjectId id);

// COMPAT(danilalexeev)
const NYPath::TYPath& GetCellCypressPathPrefix(NElection::TCellId id);
NYPath::TYPath GetCellPath(NElection::TCellId id);

const NYPath::TYPath& GetCellHydraPersistenceCypressPathPrefix(NElection::TCellId id);
NYPath::TYPath GetCellHydraPersistencePath(NElection::TCellId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
