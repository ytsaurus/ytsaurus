#pragma once

#include "public.h"

#include <yt/client/chunk_client/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriterConfig
    : public virtual NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    TFileChunkWriterConfig()
    {
        RegisterParameter("block_size", BlockSize)
            .Default(16_MB)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
