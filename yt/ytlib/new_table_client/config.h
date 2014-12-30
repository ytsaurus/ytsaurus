#pragma once

#include "public.h"

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/schema.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    i64 MaxBufferSize;

    double SampleRate;

    TChunkWriterConfig()
    {
        // Block less than 1M is nonsense.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThanOrEqual((i64) 5 * 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
public:
    bool VerifySorted;

    //ToDo(psushin): use it!
    NChunkClient::TChannels Channels;

    TChunkWriterOptions()
    {
        RegisterParameter("verify_sorted", VerifySorted)
            .Default(true);
        RegisterParameter("channels", Channels)
            .Default(NChunkClient::TChannels());
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkWriterOptions
    : public TChunkWriterOptions
    , public NChunkClient::TMultiChunkWriterOptions
{ };

DEFINE_REFCOUNTED_TYPE(TMultiChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{ };

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)


////////////////////////////////////////////////////////////////////////////////

class TTableReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{
public:
     bool SuppressAccessTracking;
     bool IgnoreUnavailableChunks;

     TTableReaderConfig()
     {
         RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
             .Default(false);

         RegisterParameter("ignore_unavailable_chunks", IgnoreUnavailableChunks)
             .Default(false);
     }
};

DEFINE_REFCOUNTED_TYPE(TTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
