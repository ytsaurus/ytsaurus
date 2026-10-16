#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TClickHouseShardTopologyState);

DECLARE_REFCOUNTED_STRUCT(TCommonClickHouseSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicCommonClickHouseSinkParameters);

DECLARE_REFCOUNTED_STRUCT(TClickHouseBatchingSinkBaseParameters);
DECLARE_REFCOUNTED_STRUCT(TClickHouseBatchingSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicClickHouseBatchingSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TShardedClickHouseBatchingSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicShardedClickHouseBatchingSinkParameters);

DECLARE_REFCOUNTED_STRUCT(TAtLeastOnceClickHouseSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicAtLeastOnceClickHouseSinkParameters);

DECLARE_REFCOUNTED_STRUCT(TAtMostOnceClickHouseSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicAtMostOnceClickHouseSinkParameters);

DECLARE_REFCOUNTED_STRUCT(TClickHouseSinkControllerParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicClickHouseSinkControllerParameters);

DECLARE_REFCOUNTED_CLASS(TClickHouseWriter);
DECLARE_REFCOUNTED_CLASS(TClickHouseBatchingSinkBase);
DECLARE_REFCOUNTED_CLASS(TClickHouseBatchingSink);
DECLARE_REFCOUNTED_CLASS(TShardedClickHouseBatchingSink);
DECLARE_REFCOUNTED_CLASS(TAtLeastOnceClickHouseSink);
DECLARE_REFCOUNTED_CLASS(TAtMostOnceClickHouseSink);
DECLARE_REFCOUNTED_CLASS(TClickHouseSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
