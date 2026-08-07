#include "public.h"

#include "arrival_order_table_sink.h"
#include "source.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_SOURCE(TSource);
YT_FLOW_DEFINE_SINK(TArrivalOrderTableSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
