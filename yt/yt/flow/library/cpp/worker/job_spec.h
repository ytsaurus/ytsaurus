#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TJobSpec
    : public NYTree::TYsonStruct
{
    TComputationSpecPtr ComputationSpec;
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
    NYTree::IMapNodePtr DynamicComputationPartitionSpec;

    //! Snapshot of dynamic pipeline spec's throttlers. Not registered —
    //! assigned by direct pointer-copy, never serialized.
    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> Throttlers;

    REGISTER_YSON_STRUCT(TDynamicJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicJobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
