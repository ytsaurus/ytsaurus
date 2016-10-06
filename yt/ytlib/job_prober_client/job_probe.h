#pragma once

#include "public.h"

namespace NYT {
namespace NJobProberClient {

////////////////////////////////////////////////////////////////////

struct IJobProbe
    : public virtual TRefCounted
{
    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() = 0;
    virtual NYson::TYsonString StraceJob() = 0;
    virtual void SignalJob(const Stroka& signalName) = 0;
    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) = 0;
    virtual Stroka GetStderr() = 0;
    virtual void Interrupt() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProbe)

////////////////////////////////////////////////////////////////////

IJobProbePtr CreateJobProbe(
    NBus::TTcpBusClientConfigPtr config,
    const NJobTrackerClient::TJobId& jobId);

////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient
} // namespace NYT
