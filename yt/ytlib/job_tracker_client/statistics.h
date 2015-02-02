#pragma once

#include "public.h"

#include <ytlib/scheduler/job.pb.h>

#include <ytlib/chunk_client/public.h>

#include <core/yson/public.h>

namespace NYT {
namespace NJobTrackerClient {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

TJobStatistics& operator += (TJobStatistics& lhs, const TJobStatistics& rhs);
TJobStatistics  operator +  (const TJobStatistics& lhs, const TJobStatistics& rhs);

TJobStatistics& operator -= (TJobStatistics& lhs, const TJobStatistics& rhs);
TJobStatistics  operator -  (const TJobStatistics& lhs, const TJobStatistics& rhs);

NChunkClient::NProto::TDataStatistics GetTotalOutputDataStatistics(const TJobStatistics& statistics);

const TJobStatistics& ZeroJobStatistics();

void Serialize(const TJobStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NJobTrackerClient
} // namespace NYT
