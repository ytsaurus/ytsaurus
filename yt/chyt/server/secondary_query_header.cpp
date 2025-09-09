#include "secondary_query_header.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void TSerializableSpanContext::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("trace_id", &TSerializableSpanContext::TraceId);
    registrar.BaseClassParameter("span_id", &TSerializableSpanContext::SpanId);
    registrar.BaseClassParameter("sampled", &TSerializableSpanContext::Sampled);
}

////////////////////////////////////////////////////////////////////////////////

void TSecondaryQueryHeader::Register(TRegistrar registrar)
{
    registrar.Parameter("query_id", &TSecondaryQueryHeader::QueryId);
    registrar.Parameter("parent_query_id", &TSecondaryQueryHeader::ParentQueryId);
    registrar.Parameter("span_context", &TSecondaryQueryHeader::SpanContext);

    registrar.Parameter("snapshot_locks", &TThis::SnapshotLocks);
    registrar.Parameter("dynamic_table_read_timestamp", &TThis::DynamicTableReadTimestamp);
    registrar.Parameter("read_transaction_id", &TThis::ReadTransactionId);

    registrar.Parameter("write_transaction_id", &TThis::WriteTransactionId);
    registrar.Parameter("created_table_path", &TThis::CreatedTablePath);

    registrar.Parameter("runtime_variables", &TThis::RuntimeVariables);

    registrar.Parameter("query_depth", &TSecondaryQueryHeader::QueryDepth);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
