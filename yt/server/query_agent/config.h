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

class TQueryAgentConfig
    : public TYsonSerializable
{
public:
    int ThreadPoolSize;
    TIntrusivePtr<TQueryChunkReaderConfig> ChunkReader;

    TQueryAgentConfig()
    {
        RegisterParameter("thread_pool_size", ThreadPoolSize)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("chunk_reader", ChunkReader)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

