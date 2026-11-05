#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

struct TExtenedMessage;

DEFINE_ENUM(ECompanionComputationType,
    ((Source)       (0))
    ((Transform)    (1))
);

DEFINE_ENUM(ECompanionResponseStatus,
    ((Ok)                       (0))
    ((Error)                    (1))
    ((JobNotFound)              (2))
    ((ResourceNotInitialized)   (4))
);

DEFINE_ENUM(ECompanionResourceCommand,
    ((Init)   (0))
    ((Unload) (1))
);

DEFINE_ENUM(ECompanionResourceExecuteStatus,
    ((Ok)                       (0))
    ((Error)                    (1))
    ((ResourceNotFound)         (2))
    ((ResourceNotInitialized)   (3))
    ((Unsupported)              (4))
    ((StaleResourceIncarnation) (5))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCompanionComputationInfo);
DECLARE_REFCOUNTED_STRUCT(TCompanionInfo);

DECLARE_REFCOUNTED_STRUCT(TCompanionPutJobRequest);
DECLARE_REFCOUNTED_STRUCT(TCompanionPutJobResponse);
DECLARE_REFCOUNTED_STRUCT(TCompanionProcessRequest);
DECLARE_REFCOUNTED_STRUCT(TCompanionResponse);
DECLARE_REFCOUNTED_STRUCT(TCompanionResourceExecuteResponse);

DECLARE_REFCOUNTED_STRUCT(ICompanionClient);
struct TCompanionJobList;
DECLARE_REFCOUNTED_CLASS(TCompanionClient);

DECLARE_REFCOUNTED_STRUCT(TCompanionConfig);
DECLARE_REFCOUNTED_STRUCT(TCompanionMonitoringConfig);
DECLARE_REFCOUNTED_STRUCT(TCompanionExecutionConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
