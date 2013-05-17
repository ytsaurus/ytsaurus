#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/compression/public.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileChunkWriterConfig
    : public virtual NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    TFileChunkWriterConfig()
    {
        RegisterParameter("block_size", BlockSize)
            .Default((i64) 16 * 1024 * 1024)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public TFileChunkWriterConfig
{ };

////////////////////////////////////////////////////////////////////////////////

struct TFileReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
