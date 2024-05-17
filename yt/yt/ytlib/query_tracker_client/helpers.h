#pragma once

#include "public.h"

#include <yt/yt/client/api/query_tracker_client.h>

#include <yt/yt/ytlib/query_tracker_client/proto/query_tracker_service.pb.h>

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

TString GetFilterFactors(const NRecords::TActiveQueryPartial& record);
TString GetFilterFactors(const NRecords::TFinishedQuery& record);
TString GetFilterFactors(const NRecords::TFinishedQueryPartial& record);

////////////////////////////////////////////////////////////////////////////////

bool IsPreFinishedState(EQueryState state);
bool IsFinishedState(EQueryState state);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQuery* protoQuery, const NApi::TQuery& query);
void FromProto(NApi::TQuery* query, const NProto::TQuery& protoQuery);

} // namespace NYT::NQueryTrackerClient
