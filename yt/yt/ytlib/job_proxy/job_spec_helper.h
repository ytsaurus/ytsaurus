#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/controller_agent/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

// Wrapper around TJobSpec, that provides convenient accessors for frequently used functions.
struct IJobSpecHelper
    : public virtual TRefCounted
{
    virtual NJobTrackerClient::EJobType GetJobType() const = 0;
    virtual const NControllerAgent::NProto::TJobSpec& GetJobSpec() const = 0;
    virtual NScheduler::TJobIOConfigPtr GetJobIOConfig() const = 0;
    virtual const NControllerAgent::NProto::TJobSpecExt& GetJobSpecExt() const = 0;
    virtual const NChunkClient::TDataSourceDirectoryPtr& GetDataSourceDirectory() const = 0;
    virtual int GetKeySwitchColumnCount() const = 0;
    virtual bool IsReaderInterruptionSupported() const = 0;
    virtual TJobTestingOptionsPtr GetJobTestingOptions() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobSpecHelper)

////////////////////////////////////////////////////////////////////////////////

IJobSpecHelperPtr CreateJobSpecHelper(const NControllerAgent::NProto::TJobSpec& jobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
