#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IJobProbe
    : public virtual TRefCounted
{
    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() = 0;

    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) = 0;

    virtual TString GetStderr() = 0;

    virtual void Interrupt() = 0;

    virtual void Fail() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProbe)

////////////////////////////////////////////////////////////////////////////////

IJobProbePtr CreateJobProbe(
    NBus::TTcpBusClientConfigPtr config,
    NJobTrackerClient::TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProberClient
