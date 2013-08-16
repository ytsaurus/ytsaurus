#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/compression/public.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriterConfig
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

class TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public TFileChunkWriterConfig
{ };

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{
public:
    bool SuppressAccessTracking;

    TFileReaderConfig()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
