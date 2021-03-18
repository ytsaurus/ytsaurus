#include "subquery_header.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TSubqueryHeader::TSubqueryHeader()
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
