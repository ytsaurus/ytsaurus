#pragma once

#include "public.h"
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

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
};

DEFINE_REFCOUNTED_TYPE(ICompanionClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
