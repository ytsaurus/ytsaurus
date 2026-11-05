#include "public.h"

#include "passthrough_computation.h"
#include "static_table_key_visitor_joiner.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_COMPUTATION(TPassthroughComputation);
YT_FLOW_DEFINE_COMPUTATION(TSwiftPassthroughOrderedSourceComputation);
YT_FLOW_DEFINE_COMPUTATION(TSwiftPassthroughComputation);

YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER(TStaticTableKeyVisitorJoiner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
