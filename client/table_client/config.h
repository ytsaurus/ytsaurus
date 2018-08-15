#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/client/chunk_client/config.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TRetentionConfig
    : public NYTree::TYsonSerializable
{
public:
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;
    bool IgnoreMajorTimestamp;

    TRetentionConfig()
    {
        RegisterParameter("min_data_versions", MinDataVersions)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("max_data_versions", MaxDataVersions)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("min_data_ttl", MinDataTtl)
            .Default(TDuration::Minutes(30));
        RegisterParameter("max_data_ttl", MaxDataTtl)
            .Default(TDuration::Minutes(30));
        RegisterParameter("ignore_major_timestamp", IgnoreMajorTimestamp)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

TString ToString(const TRetentionConfigPtr& obj);

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
{
public:
    i64 MaxDataSizePerRead;

    TNullable<double> SamplingRate;
    TNullable<ui64> SamplingSeed;

    TChunkReaderConfig()
    {
        RegisterParameter("max_data_size_per_read", MaxDataSizePerRead)
            .GreaterThanOrEqual((i64) 1024 * 1024)
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

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    i64 MaxBufferSize;

    i64 MaxRowWeight;

    i64 MaxKeyWeight;

    //! This limits ensures that chunk index is dense enough
    //! e.g. to produce good slices for reduce.
    i64 MaxDataWeightBetweenBlocks;

    i64 MaxKeyFilterSize;

    double SampleRate;

    double KeyFilterFalsePositiveRate;

    TChunkWriterConfig()
    {
        // Allow very small blocks for testing purposes.
        RegisterParameter("block_size", BlockSize)
            .GreaterThan(0)
            .Default(16_MB);

        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThan(0)
            .Default(16_MB);

        RegisterParameter("max_row_weight", MaxRowWeight)
            .GreaterThanOrEqual(5_MB)
            .LessThanOrEqual(MaxRowWeightLimit)
            .Default(16_MB);

        RegisterParameter("max_key_weight", MaxKeyWeight)
            .GreaterThan(0)
            .LessThanOrEqual(MaxKeyWeightLimit)
            .Default(16_KB);

        RegisterParameter("max_data_weight_between_blocks", MaxDataWeightBetweenBlocks)
            .GreaterThan(0)
            .Default(2_GB);

        RegisterParameter("max_key_filter_size", MaxKeyFilterSize)
            .GreaterThan(0)
            .LessThanOrEqual(1_MB)
            .Default(64_KB);

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

class TTableReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
    , public virtual TChunkReaderConfig
{
public:
    bool SuppressAccessTracking;
    EUnavailableChunkStrategy UnavailableChunkStrategy;

    TTableReaderConfig()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
        RegisterParameter("unavailable_chunk_strategy", UnavailableChunkStrategy)
            .Default(EUnavailableChunkStrategy::Restore);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{
public:
    TDuration UploadTransactionTimeout;

    TTableWriterConfig()
    {
        RegisterParameter("upload_transaction_timeout", UploadTransactionTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
