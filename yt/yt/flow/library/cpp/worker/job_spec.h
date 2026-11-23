#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TJobSpec
    : public NYTree::TYsonStruct
{
    TComputationSpecPtr ComputationSpec;
    bool FailureHandlerEnabled = false;
    TExtendedComputationSpecPtr ExtendedComputationSpec;

    TJobPtr Job;
    TPartitionPtr Partition;

    REGISTER_YSON_STRUCT(TJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicJobSpec
    : public NYTree::TYsonStruct
{
    TDynamicComputationSpecPtr DynamicComputationSpec;
    TDynamicPartitionSpecPtr DynamicComputationPartitionSpec;

    //! Pipeline throttler specs visible to user computations.
    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> Throttlers;

    REGISTER_YSON_STRUCT(TDynamicJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicJobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
