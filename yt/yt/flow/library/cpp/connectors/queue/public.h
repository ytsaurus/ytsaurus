#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

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
