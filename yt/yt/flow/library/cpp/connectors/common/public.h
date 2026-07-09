#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IOrderedSource);
DECLARE_REFCOUNTED_STRUCT(TOrderedSourcePartitionState);

DECLARE_REFCOUNTED_STRUCT(TSyncSinkState);

DECLARE_REFCOUNTED_STRUCT(TOrderedAsyncSinkState);

DECLARE_REFCOUNTED_STRUCT(TOrderedBatchingAsyncSinkState);

DECLARE_REFCOUNTED_STRUCT(TDelegatingAsyncSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDelegatingAsyncSinkDynamicParameters);
DECLARE_REFCOUNTED_STRUCT(TAtMostOnceStrategyParameters);
DECLARE_REFCOUNTED_STRUCT(TAtMostOnceStrategyDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
