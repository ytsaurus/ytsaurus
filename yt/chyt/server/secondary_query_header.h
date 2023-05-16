#pragma once

#include "private.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TSerializableSpanContext
    : public NYTree::TYsonStruct
    , public NTracing::TSpanContext
{
public:
    REGISTER_YSON_STRUCT(TSerializableSpanContext);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSerializableSpanContext)

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQueryHeader
    : public NYTree::TYsonStruct
{
public:
    TQueryId QueryId;
    TQueryId ParentQueryId;
    TSerializableSpanContextPtr SpanContext;
    NTransactionClient::TTransactionId WriteTransactionId;
    std::optional<NYPath::TYPath> CreatedTablePath;
    // These values should always be initialized explicitly.
    // Set default values for easier debugging if we forget to initialize them.
    int StorageIndex = -42;
    int QueryDepth = -42;

    REGISTER_YSON_STRUCT(TSecondaryQueryHeader);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecondaryQueryHeader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
