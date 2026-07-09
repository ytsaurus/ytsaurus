#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TServiceLogParameters
    : public TOrderedSourceBase::TParameters
{
    TTableJoinerSpecPtr TableJoiner;

    REGISTER_YSON_STRUCT(TServiceLogParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceLogParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicServiceLogParameters
    : public TOrderedSourceBase::TDynamicParameters
{
    i64 DesiredPartitionCount{};
    TDuration DesiredCycleTime;

    TDuration ThrottlerPeriod;

    REGISTER_YSON_STRUCT(TDynamicServiceLogParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicServiceLogParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicServiceLogPartitionSpec
    : public TOrderedSourceBase::TDynamicPartitionSpec
{
    TServiceLogRangePtr Range;

    REGISTER_YSON_STRUCT(TDynamicServiceLogPartitionSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicServiceLogPartitionSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
