#include "public.h"

#include "sink.h"
#include "source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_SOURCE(TQueueSource);

YT_FLOW_DEFINE_SINK(TAsyncQueueSink);
YT_FLOW_DEFINE_SINK(TAsyncMultiClusterQueueSink);
YT_FLOW_DEFINE_SINK(TSyncQueueSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
