#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Restores canonical data leaves in a job proxy statistics snapshot.
void AddJobDataStatistics(
    TNonNullPtr<TStatistics> statistics,
    const NChunkClient::NProto::TDataStatistics& inputDataStatistics,
    const std::vector<NChunkClient::NProto::TDataStatistics>& outputDataStatistics,
    bool hasInput,
    int outputCount);

//! Copies a prepared payload and its decoding metadata into a fresh job status.
void EncodeJobStatisticsForHeartbeat(
    TNonNullPtr<NProto::TJobStatus> status,
    const NYson::TYsonString& statisticsYson,
    EJobStatisticsWireFlags wireFlags,
    bool inputDataStatisticsOmitted,
    int outputDataStatisticsOmittedCount);

//! Decodes a present job statistics payload and restores omitted data leaves.
TStatistics DecodeJobStatisticsFromHeartbeat(const NProto::TJobStatus& status);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
