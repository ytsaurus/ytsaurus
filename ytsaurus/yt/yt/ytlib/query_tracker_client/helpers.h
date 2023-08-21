#pragma once

#include "public.h"

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

TString GetFilterFactors(const NRecords::TActiveQueryPartial& record);
TString GetFilterFactors(const NRecords::TFinishedQuery& record);
TString GetFilterFactors(const NRecords::TFinishedQueryPartial& record);

////////////////////////////////////////////////////////////////////////////////

bool IsPreFinishedState(EQueryState state);
bool IsFinishedState(EQueryState state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
