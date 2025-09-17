#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

TString FormatAcoList(const std::optional<NYson::TYsonString>& accessControlObjects);

////////////////////////////////////////////////////////////////////////////////

std::string GetFilterFactors(const NRecords::TActiveQueryPartial& record);
std::string GetFilterFactors(const NRecords::TFinishedQuery& record);
std::string GetFilterFactors(const NRecords::TFinishedQueryPartial& record);
std::string GetFilterFactors(const NApi::TQuery& record);

////////////////////////////////////////////////////////////////////////////////

bool IsFinishingState(EQueryState state);
bool IsFinishedState(EQueryState state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
