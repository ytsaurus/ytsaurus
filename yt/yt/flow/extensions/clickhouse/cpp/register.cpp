#include "sink.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_SINK(TClickHouseBatchingSink);
YT_FLOW_DEFINE_SINK(TShardedClickHouseBatchingSink);
YT_FLOW_DEFINE_SINK(TAtLeastOnceClickHouseSink);
YT_FLOW_DEFINE_SINK(TAtMostOnceClickHouseSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
