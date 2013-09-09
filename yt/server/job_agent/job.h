#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/signal.h>

#include <ytlib/node_tracker_client/node.pb.h>

#include <ytlib/job_tracker_client/job.pb.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct IJob
    : public virtual TRefCounted
{
    DECLARE_INTERFACE_SIGNAL(void(), ResourcesReleased);

    virtual void Start() = 0;

    virtual void Abort(const TError& error) = 0;

    virtual const TJobId& GetId() const = 0;

    virtual const NJobTrackerClient::NProto::TJobSpec& GetSpec() const = 0;

    virtual EJobState GetState() const = 0;

    virtual EJobPhase GetPhase() const = 0;

    virtual NNodeTrackerClient::NProto::TNodeResources GetResourceUsage() const = 0;

    /*!
     *  New usage should not exceed the previous one.
     */
    virtual void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& newUsage) = 0;

    virtual NJobTrackerClient::NProto::TJobResult GetResult() const = 0;
    virtual void SetResult(const NJobTrackerClient::NProto::TJobResult& result) = 0;

    virtual double GetProgress() const = 0;
    virtual void SetProgress(double value) = 0;

    virtual NJobTrackerClient::NProto::TJobStatistics GetJobStatistics() const = 0;
    virtual void SetJobStatistics(const NJobTrackerClient::NProto::TJobStatistics& statistics) = 0;

};

typedef
    TCallback<
        IJobPtr(
            const TJobId& jobId,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec)
    > TJobFactory;

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT

