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

    //! Applicable to versioned chunk writer.
    i64 MaxSizePerIndexEntry;

    TChunkWriterConfig()
    {
        // Block less than 1M is nonsense.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_size_per_index_entry", MaxSizePerIndexEntry)
            .GreaterThanOrEqual((i64) 1024)
            .Default((i64)32 * 1024);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderConfig
    : public NChunkClient::TSequentialReaderConfig
{
public:
    TChunkReaderConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
