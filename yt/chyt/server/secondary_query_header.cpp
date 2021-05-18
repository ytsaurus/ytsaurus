#include "secondary_query_header.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TSecondaryQueryHeader::TSecondaryQueryHeader()
{
    RegisterParameter("query_id", QueryId);
    RegisterParameter("parent_query_id", ParentQueryId);
    
    RegisterParameter("trace_id", SpanContext.TraceId);
    RegisterParameter("span_id", SpanContext.SpanId);
    RegisterParameter("sampled", SpanContext.Sampled);
    
    RegisterParameter("storage_index", StorageIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
