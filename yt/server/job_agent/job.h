#pragma once

#include "public.h"
#include "job_statistics.h"
#include "statistics_reporter.h"
#include "gpu_manager.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/ytlib/job_prober_client/public.h>

#include <yt/ytlib/node_tracker_client/proto/node.pb.h>

#include <yt/core/actions/signal.h>

#include <yt/core/misc/error.h>

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJob
    : public virtual TRefCounted
{
    DECLARE_INTERFACE_SIGNAL(void(
        const NNodeTrackerClient::NProto::TNodeResources& resourceDelta),
        ResourcesUpdated);

    DECLARE_INTERFACE_SIGNAL(void(), PortsReleased);

    virtual void Start() = 0;

    virtual void Abort(const TError& error) = 0;
    virtual void Fail() = 0;

    virtual const TJobId& GetId() const = 0;
    virtual const TOperationId& GetOperationId() const = 0;

    virtual EJobType GetType() const = 0;

    virtual const NJobTrackerClient::NProto::TJobSpec& GetSpec() const = 0;

    virtual int GetPortCount() const = 0;

    virtual EJobState GetState() const = 0;

    virtual EJobPhase GetPhase() const = 0;

    virtual NNodeTrackerClient::NProto::TNodeResources GetResourceUsage() const = 0;
    virtual std::vector<int> GetPorts() const = 0;
    virtual void SetPorts(const std::vector<int>& ports) = 0;

    virtual void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& newUsage) = 0;

    virtual NJobTrackerClient::NProto::TJobResult GetResult() const = 0;
    virtual void SetResult(const NJobTrackerClient::NProto::TJobResult& result) = 0;

    virtual double GetProgress() const = 0;
    virtual void SetProgress(double value) = 0;

    virtual ui64 GetStderrSize() const = 0;
    virtual void SetStderrSize(ui64 value) = 0;

    virtual void SetStderr(const TString& value) = 0;
    virtual void SetFailContext(const TString& value) = 0;

    virtual NYson::TYsonString GetStatistics() const = 0;
    virtual void SetStatistics(const NYson::TYsonString& statistics) = 0;

    virtual void OnJobPrepared() = 0;

    virtual TInstant GetStartTime() const = 0;
    virtual TNullable<TDuration> GetPrepareDuration() const = 0;
    virtual TNullable<TDuration> GetDownloadDuration() const = 0;
    virtual TNullable<TDuration> GetExecDuration() const = 0;

    virtual TInstant GetStatisticsLastSendTime() const = 0;
    virtual void ResetStatisticsLastSendTime() = 0;

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() = 0;
    virtual TString GetStderr() = 0;
    virtual TNullable<TString> GetFailContext() = 0;
    virtual NYson::TYsonString StraceJob() = 0;
    virtual void SignalJob(const TString& signalName) = 0;
    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) = 0;

    virtual bool GetStored() const = 0;
    virtual void SetStored(bool value) = 0;

    virtual void ReportStatistics(TJobStatistics&& statistics) = 0;
    virtual void ReportSpec() = 0;
    virtual void ReportStderr() = 0;
    virtual void ReportFailContext() = 0;

    virtual void Interrupt() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJob)

using TJobFactory = TCallback<IJobPtr(
    const TJobId& jobId,
    const TOperationId& operationId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    NJobTrackerClient::NProto::TJobSpec&& jobSpec)>;

////////////////////////////////////////////////////////////////////////////////

void FillJobStatus(NJobTrackerClient::NProto::TJobStatus* jobStatus, IJobPtr job);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
