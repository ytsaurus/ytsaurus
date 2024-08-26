#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_proxy/proto/job_prober_service.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobIOOrchidInfo
{
    // For some jobs this value might not make sense.
    // For testing purposes default from JobSpec is supplied.
    i64 BufferRowCount;

    void BuildOrchid(NYTree::TFluentAny fluent) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyOrchidInfo
{
    TJobIOOrchidInfo JobIOInfo;

    void BuildOrchid(NYTree::TFluentAny fluent) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
