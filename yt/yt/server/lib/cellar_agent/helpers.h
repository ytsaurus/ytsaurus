#pragma once

#include "public.h"

#include <yt/yt/client/election/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

NCellarClient::ECellarType GetCellarTypeFromCellId(NElection::TCellId id);
NCellarClient::ECellarType GetCellarTypeFromCellBundleId(NObjectClient::TObjectId id);
const TString& GetCellCypressPrefix(NElection::TCellId id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
