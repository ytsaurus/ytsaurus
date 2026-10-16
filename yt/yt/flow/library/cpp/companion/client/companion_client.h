#pragma once

#include "public.h"
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Reply of ICompanionClient::ListJobs.
struct TCompanionJobList
{
    std::vector<TJobId> JobIds;
    //! Pid of the answering process; distinguishes fan-out children.
    i64 ProcessId = 0;
};

//! Interface for communication with Companion process.
struct ICompanionClient
    : public TRefCounted
{
    //! Delegate DoProcess from Computation to Companion.
    virtual TCompanionResponsePtr DoProcessWithCompanionSync(
        const TCompanionProcessRequestPtr& companionRequest,
        const IExternalPerformanceMetricsReporterPtr& reporter) = 0;

    //! Requests current status information from Companion: computations, types of computation etc.
    virtual TCompanionInfoPtr GetCompanionInfo() = 0;

    //! Create or replace job in Companion.
    virtual TCompanionPutJobResponsePtr PutJob(
        const TCompanionPutJobRequestPtr& putJobRequest,
        const IExternalPerformanceMetricsReporterPtr& reporter) = 0;

    //! Removes job from Companion. One attempt; the caller owns the retries.
    virtual TFuture<void> RemoveJob(const TJobId& jobId) = 0;

    //! Jobs held by the companion process this client's channel currently
    //! reaches. One attempt; the caller owns the retries.
    virtual TFuture<TCompanionJobList> ListJobs() = 0;

    //! Executes a lifecycle command over a resource hosted in Companion.
    virtual TFuture<TCompanionResourceExecuteResponsePtr> ResourceExecute(
        const TResourceId& resourceId,
        ECompanionResourceCommand command,
        const NYson::TYsonString& argument) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompanionClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
