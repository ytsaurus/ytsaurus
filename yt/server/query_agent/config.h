#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/compression/public.h>

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
    : public NYTree::TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentRequests;

    NCompression::ECodec LookupResponseCodec;
    NCompression::ECodec SelectResponseCodec;

    TIntrusivePtr<TQueryChunkReaderConfig> Reader;

    TQueryAgentConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("max_concurrent_requests", MaxConcurrentRequests)
            .GreaterThan(0)
            .Default(4);

        RegisterParameter("lookup_response_codec", LookupResponseCodec)
            .Default(NCompression::ECodec::Lz4);
        RegisterParameter("select_response_codec", SelectResponseCodec)
            .Default(NCompression::ECodec::Lz4);

        RegisterParameter("reader", Reader)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

