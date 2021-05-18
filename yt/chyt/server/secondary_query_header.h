#pragma once

#include "private.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQueryHeader
    : public NYTree::TYsonSerializable
{
public:
    TQueryId QueryId;
    TQueryId ParentQueryId;
    NTracing::TSpanContext SpanContext;
    // These values should always be initialized explicitly.
    // Set default values for easier debugging if we forget to initialize them.
    int StorageIndex = -42;
    int QueryDepth = -42;

    TSecondaryQueryHeader();
};

DEFINE_REFCOUNTED_TYPE(TSecondaryQueryHeader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
