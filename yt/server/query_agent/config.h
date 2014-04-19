#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryChunkReaderConfig
    : public NVersionedTableClient::TChunkReaderConfig
    , public NChunkClient::TReplicationReaderConfig
{ };

DEFINE_REFCOUNTED_TYPE(TQueryChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueryAgentConfig
    : public TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentRequests;

    TIntrusivePtr<TQueryChunkReaderConfig> Reader;

    TQueryAgentConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("max_concurrent_requests", MaxConcurrentRequests)
            .GreaterThan(0)
            .Default(4);

        RegisterParameter("reader", Reader)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

