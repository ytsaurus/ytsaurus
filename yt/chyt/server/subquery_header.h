#pragma once

#include "private.h"

#include <yt/yt/core/tracing/trace_context.h>
#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TSubqueryHeader
    : public NYTree::TYsonSerializable
{
public:
    TQueryId QueryId;
    TQueryId ParentQueryId;
    NTracing::TSpanContext SpanContext;
    int StorageIndex;

    TSubqueryHeader();
};

DEFINE_REFCOUNTED_TYPE(TSubqueryHeader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
