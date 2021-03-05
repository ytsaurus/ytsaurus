#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::NProto::TDataStatistics GetTotalInputDataStatistics(const TStatistics& jobStatistics);
NChunkClient::NProto::TDataStatistics GetTotalOutputDataStatistics(const TStatistics& jobStatistics);

THashMap<int, NChunkClient::NProto::TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics);
THashMap<int, i64> GetOutputPipeIdleTimes(const TStatistics& jobStatistics);

extern const TString ExecAgentTrafficStatisticsPrefix;
extern const TString JobProxyTrafficStatisticsPrefix;

void FillTrafficStatistics(
    const TString& namePrefix,
    TStatistics& statistics,
    const NChunkClient::TTrafficMeterPtr& trafficMeter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
