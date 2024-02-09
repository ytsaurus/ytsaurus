#pragma once

#include "public.h"

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/bus/tcp/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IJobProbe
    : public virtual TRefCounted
{
    virtual std::vector<NChunkClient::TChunkId> DumpInputContext(NTransactionClient::TTransactionId transactionId) = 0;

    virtual NApi::TPollJobShellResponse PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters) = 0;

    virtual TString GetStderr() = 0;

    virtual void Interrupt() = 0;

    virtual void GracefulAbort(TError error) = 0;

    virtual void Fail() = 0;

    virtual TSharedRef DumpSensors() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProbe)

////////////////////////////////////////////////////////////////////////////////

IJobProbePtr CreateJobProbe(
    NBus::TBusClientConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
