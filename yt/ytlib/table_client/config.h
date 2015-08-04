#pragma once

#include "public.h"

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/schema.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    i64 MaxBufferSize;

    i64 MaxRowWeight;

    i64 MaxKeyFilterSize;

    double SampleRate;

    double KeyFilterFalsePositiveRate;

    TChunkWriterConfig()
    {
        // Allow very small blocks for testing purposes.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThanOrEqual((i64) 5 * 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_row_weight", MaxRowWeight)
            .GreaterThanOrEqual((i64) 5 * 1024 * 1024)
            .LessThanOrEqual((i64) 128 * 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_key_filter_size", MaxKeyFilterSize)
            .GreaterThan((i64) 0)
            .LessThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 64 * 1024);

        RegisterParameter("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);

        RegisterParameter("key_filter_false_positive_rate", KeyFilterFalsePositiveRate)
            .GreaterThan(0)
            .LessThanOrEqual(1.0)
            .Default(0.03);
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

class TTableWriterOptions
    : public TChunkWriterOptions
    , public NChunkClient::TMultiChunkWriterOptions
{ };

DEFINE_REFCOUNTED_TYPE(TTableWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{ };

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TBufferedTableWriterConfig
    : public TTableWriterConfig
{
public:
    TDuration RetryBackoffTime;
    TDuration FlushPeriod;

    TBufferedTableWriterConfig()
    {
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TBufferedTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderConfig
    : public virtual NChunkClient::TSequentialReaderConfig
{
public:
    i64 MaxDataSizePerRead;

    TNullable<double> SamplingRate;
    TNullable<ui64> SamplingSeed;

    TChunkReaderConfig()
    {
        RegisterParameter("max_data_size_per_read", MaxDataSizePerRead)
            .GreaterThan((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("sampling_rate", SamplingRate)
            .Default()
            .InRange(0, 1);

        RegisterParameter("sampling_seed", SamplingSeed)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
    , public TChunkReaderConfig
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

class TControlAttributesConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTableIndex;
    bool EnableKeySwitch;
    bool EnableRangeIndex;
    bool EnableRowIndex;

    TControlAttributesConfig()
    {
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);

        RegisterParameter("enable_key_switch", EnableKeySwitch)
            .Default(false);

        RegisterParameter("enable_range_index", EnableRangeIndex)
            .Default(false);

        RegisterParameter("enable_row_index", EnableRowIndex)
            .Default(false);

        RegisterValidator([&] () {
            if (EnableRangeIndex && !EnableRowIndex) {
                THROW_ERROR_EXCEPTION("\"enable_row_index\" must be set when \"enable_range_index\" is set");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TControlAttributesConfig);

//////////////////////////////////////////////////////////////////////////////////

class TRetentionConfig
    : public NYTree::TYsonSerializable
{
public:
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;

    TRetentionConfig()
    {
        RegisterParameter("min_data_versions", MinDataVersions)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("max_data_versions", MaxDataVersions)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("min_data_ttl", MinDataTtl)
            .Default(TDuration::Minutes(5));
        RegisterParameter("max_data_ttl", MaxDataTtl)
            .Default(TDuration::Minutes(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
