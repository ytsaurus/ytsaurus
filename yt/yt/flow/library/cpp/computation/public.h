#pragma once

#include "job_state/public.h"
#include "stores/public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TComputationBase;
DECLARE_REFCOUNTED_STRUCT(IMessageFilter);
DECLARE_REFCOUNTED_STRUCT(IOutputCollector);

DECLARE_REFCOUNTED_STRUCT(TOutputStoreStreamOrchidState);
DECLARE_REFCOUNTED_STRUCT(TUniversalComputationOrchidState);
DECLARE_REFCOUNTED_STRUCT(TUniversalComputationDynamicPartitionSpec);
DECLARE_REFCOUNTED_STRUCT(TUniversalComputationPartitionStatus);

DECLARE_REFCOUNTED_CLASS(TRootOutputCollector);
DECLARE_REFCOUNTED_STRUCT(IOutputCollector);

DECLARE_REFCOUNTED_STRUCT(TOffsetStoreContext);

DECLARE_REFCOUNTED_STRUCT(IMetaSetter);
DECLARE_REFCOUNTED_STRUCT(TMessageParents);
using TMessageParentsConstPtr = TIntrusivePtr<const TMessageParents>;

DECLARE_REFCOUNTED_STRUCT(IWatermarkAligner);
DECLARE_REFCOUNTED_STRUCT(IEventTimestampAssigner);

DECLARE_REFCOUNTED_STRUCT(TWatermarkGeneratorState);
DECLARE_REFCOUNTED_STRUCT(IWatermarkGenerator);

DECLARE_REFCOUNTED_STRUCT(TSwiftOrderedSourceTimestampMemory);

DECLARE_REFCOUNTED_STRUCT(TSimpleExternalStateManagerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicSimpleExternalStateManagerSpec);
DECLARE_REFCOUNTED_CLASS(TSimpleExternalStateManager);

DECLARE_REFCOUNTED_STRUCT(TSimpleExternalStateJoinerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicSimpleExternalStateJoinerSpec);
DECLARE_REFCOUNTED_CLASS(TSimpleExternalStateJoiner);

DECLARE_REFCOUNTED_STRUCT(TStaticTableKeyVisitorJoinerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicStaticTableKeyVisitorJoinerSpec);
DECLARE_REFCOUNTED_CLASS(TStaticTableKeyVisitorJoiner);

DECLARE_REFCOUNTED_CLASS(TPassthroughComputation);
DECLARE_REFCOUNTED_CLASS(TSwiftPassthroughComputation);
DECLARE_REFCOUNTED_CLASS(TSwiftPassthroughOrderedSourceComputation);

DECLARE_REFCOUNTED_STRUCT(IOrderedSource);
DECLARE_REFCOUNTED_STRUCT(TOrderedSourcePartitionState);

DECLARE_REFCOUNTED_CLASS(IComputationTracer);

DEFINE_ENUM(EProcessingMode,
    ((ExactlyOnce)              (0))
    ((AtLeastOnceConsistent)    (1))
);

DECLARE_REFCOUNTED_STRUCT(TUniversalComputationControllerState);
DECLARE_REFCOUNTED_CLASS(TUniversalComputationController);

DECLARE_REFCOUNTED_STRUCT(TKeyVisitorInterval);
DECLARE_REFCOUNTED_CLASS(TKeyVisitorStore);
DECLARE_REFCOUNTED_STRUCT(TKeyVisitorContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicKeyVisitorContext);
DECLARE_REFCOUNTED_CLASS(TKeyVisitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
