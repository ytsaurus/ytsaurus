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
    ((Ok)           (0))
    ((Error)        (1))
    ((JobNotFound)  (2))
);

DECLARE_REFCOUNTED_STRUCT(TCompanionComputationInfo);
DECLARE_REFCOUNTED_STRUCT(TCompanionInfo);

DECLARE_REFCOUNTED_STRUCT(TCompanionPutJobRequest);
DECLARE_REFCOUNTED_STRUCT(TCompanionPutJobResponse);
DECLARE_REFCOUNTED_STRUCT(TCompanionProcessRequest);
DECLARE_REFCOUNTED_STRUCT(TCompanionResponse);

DECLARE_REFCOUNTED_STRUCT(ICompanionClient);
DECLARE_REFCOUNTED_CLASS(TCompanionClient);

DECLARE_REFCOUNTED_CLASS(TCompanionManager);
DECLARE_REFCOUNTED_CLASS(TJavaCompanionManager);

DECLARE_REFCOUNTED_CLASS(TSwiftMapCompanionComputation);
DECLARE_REFCOUNTED_CLASS(TSwiftOrderedSourceCompanionComputation);
DECLARE_REFCOUNTED_CLASS(TTransformCompanionComputation);

DECLARE_REFCOUNTED_STRUCT(TCompanionConfig);
DECLARE_REFCOUNTED_STRUCT(TCompanionExecutionConfig);
DECLARE_REFCOUNTED_STRUCT(TCompanionEntrypoint);
DECLARE_REFCOUNTED_STRUCT(TCompanionManagerParameters);
DECLARE_REFCOUNTED_STRUCT(TJavaCompanionManagerParameters);

DECLARE_REFCOUNTED_CLASS(TJavaProcessManager);
DECLARE_REFCOUNTED_CLASS(TProcessManagerBase);
DECLARE_REFCOUNTED_CLASS(TCompanionProcessManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
