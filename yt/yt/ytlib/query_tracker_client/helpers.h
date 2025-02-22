#pragma once

#include "public.h"

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

std::string GetFilterFactors(const NRecords::TActiveQueryPartial& record);
std::string GetFilterFactors(const NRecords::TFinishedQuery& record);
std::string GetFilterFactors(const NRecords::TFinishedQueryPartial& record);

////////////////////////////////////////////////////////////////////////////////

bool IsPreFinishedState(EQueryState state);
bool IsFinishedState(EQueryState state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
