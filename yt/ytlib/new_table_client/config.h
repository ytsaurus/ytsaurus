#pragma once

#include "public.h"

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    TChunkWriterConfig()
    {
        // Block less than 1M is nonsense.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);
    }
};

class TChunkReaderConfig
    : public NChunkClient::TSequentialReaderConfig
{
public:

    TChunkReaderConfig()
    {
        /*
        // Block less than 1M is nonsense.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);
        */
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
