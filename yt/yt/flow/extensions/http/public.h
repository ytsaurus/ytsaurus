#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

DECLARE_REFCOUNTED_STRUCT(TAsyncHttpSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicAsyncHttpSinkParameters);
DECLARE_REFCOUNTED_CLASS(TAsyncHttpRequestExecutor);
DECLARE_REFCOUNTED_CLASS(TAsyncHttpSinkController);
DECLARE_REFCOUNTED_CLASS(TAsyncHttpSink);

} // namespace NYT::NFlow
