#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(max42): these methods are legacy and must be removed when their last usage is removed.
NChunkClient::NProto::TDataStatistics GetTotalInputDataStatistics(const TStatistics& jobStatistics);
std::vector<NChunkClient::NProto::TDataStatistics> GetOutputDataStatistics(const TStatistics& jobStatistics);

THashMap<int, i64> GetOutputPipeIdleTimes(const TStatistics& jobStatistics);

// TODO(pogorelov): move to CA
extern const TString ExecAgentTrafficStatisticsPrefix;
extern const TString JobProxyTrafficStatisticsPrefix;

void FillTrafficStatistics(
    const TString& namePrefix,
    TStatistics& statistics,
    const NChunkClient::TTrafficMeterPtr& trafficMeter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
