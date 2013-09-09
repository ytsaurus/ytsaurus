#pragma once

#include "public.h"

#include <ytlib/scheduler/job.pb.h>
#include <core/yson/public.h>

namespace NYT {
namespace NJobTrackerClient {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

NProto::TJobStatistics& operator += (NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs);
NProto::TJobStatistics  operator +  (const NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs);

NProto::TJobStatistics& operator -= (NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs);
NProto::TJobStatistics  operator -  (const NProto::TJobStatistics& lhs, const NProto::TJobStatistics& rhs);

const NProto::TJobStatistics& ZeroJobStatistics();

void Serialize(const NProto::TJobStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

} // namespace NJobTrackerClient
} // namespace NYT
