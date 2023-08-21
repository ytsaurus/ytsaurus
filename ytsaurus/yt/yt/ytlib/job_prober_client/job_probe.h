#pragma once

#include "public.h"
#include "job_shell_descriptor_cache.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/bus/tcp/public.h>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IJobProbe
    : public virtual TRefCounted
{
    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() = 0;

    virtual NApi::TPollJobShellResponse PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters) = 0;

    virtual TString GetStderr() = 0;

    virtual void Interrupt() = 0;

    virtual void Fail() = 0;

    virtual TSharedRef DumpSensors() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProbe)

////////////////////////////////////////////////////////////////////////////////

IJobProbePtr CreateJobProbe(
    NBus::TBusClientConfigPtr config,
    NJobTrackerClient::TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProberClient
