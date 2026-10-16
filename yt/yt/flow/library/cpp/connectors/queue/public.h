#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Reduces the tablet_index_routing_hash_expression uint64 hash to a queue tablet index.
DEFINE_ENUM(EQueueTabletIndexRoutingHashPolicy,
    // Reduce the hash to contiguous equal-width ranges (rangeSize = 2^64 / tabletCount). Recommended:
    // a consumer range-partitioned by the same key reads only its own tablet.
    ((Range)  (0))
    // Reduce as hash % tabletCount. Discouraged: Flow computations are range-partitioned by key, so a
    // modulo-partitioned queue forces every reader to read every tablet (a full mesh on read).
    ((Modulo) (1))
);

DECLARE_REFCOUNTED_CLASS(TTabletIndexEvaluator);
DECLARE_REFCOUNTED_CLASS(TTabletRouter);

DECLARE_REFCOUNTED_STRUCT(TQueueSinkTabletRoutingParameters);

DECLARE_REFCOUNTED_STRUCT(TQueueInfoSpec);
DECLARE_REFCOUNTED_STRUCT(TQueueInfoControllerState);
DECLARE_REFCOUNTED_CLASS(TQueueInfoController);

DECLARE_REFCOUNTED_STRUCT(TQueueSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicQueueSourceParameters);
DECLARE_REFCOUNTED_CLASS(TQueueSource);
DECLARE_REFCOUNTED_CLASS(TQueueSourceController);

DECLARE_REFCOUNTED_STRUCT(TCommonQueueSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicCommonQueueSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TSyncQueueSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicSyncQueueSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TAsyncQueueWriterParameters);
DECLARE_REFCOUNTED_STRUCT(TAsyncMultiClusterQueueWriterParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicAsyncQueueWriterParameters);
DECLARE_REFCOUNTED_STRUCT(TAsyncQueueSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicAsyncQueueSinkParameters);

DECLARE_REFCOUNTED_CLASS(TSyncQueueSink);
DECLARE_REFCOUNTED_CLASS(IAsyncQueueWriter);
DECLARE_REFCOUNTED_CLASS(TAsyncQueueWriterBase);
DECLARE_REFCOUNTED_CLASS(TAsyncQueueWriter);
DECLARE_REFCOUNTED_CLASS(TAsyncMultiClusterQueueWriter);
DECLARE_REFCOUNTED_CLASS(TAsyncQueueSinkImpl);
DECLARE_REFCOUNTED_CLASS(TAsyncQueueSink);
DECLARE_REFCOUNTED_CLASS(TAsyncMultiClusterQueueSink);

DECLARE_REFCOUNTED_CLASS(TQueueSinkController);
DECLARE_REFCOUNTED_CLASS(TMultiClusterQueueSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
