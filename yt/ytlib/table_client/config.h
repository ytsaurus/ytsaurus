
#pragma once

#include <ytlib/misc/codec.h>
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableConsumerConfig
    : public TConfigurable
{
    i64 MaxColumnNameSize;
    i64 MaxRowSize;
    i64 MaxKeySize;

    TTableConsumerConfig() 
    {
        Register("max_column_name_size", MaxColumnNameSize)
            .LessThanOrEqual(256)
            .Default(256);
        Register("max_row_size", MaxRowSize)
            .LessThanOrEqual(16 * 1024 * 1024)
            .Default(16 * 1024 * 1024);
        Register("max_key_size", MaxKeySize)
            .LessThanOrEqual(4 * 1024)
            .Default(4 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterConfig
    : public TConfigurable
{
    i64 BlockSize;
    int CodecId;

    //! Size of samples should not exceed given percent of the total data size.
    double SampleRate;

    //! Size of index should not exceed given percent of the total data size.
    double IndexRate;

    TChunkWriterConfig()
    {
        // Block less than 1Kb is a nonsense.
        Register("block_size", BlockSize)
            .GreaterThan(1024)
            .Default(1024 * 1024);
        Register("codec_id", CodecId)
            .Default(ECodecId::Snappy);
        Register("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThan(0.1)
            .Default(0.01);
        Register("index_rate", IndexRate)
            .GreaterThan(0)
            .LessThan(0.1)
            .Default(0.01);
        
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceWriterConfig
    : public TConfigurable
{
};

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceReaderConfig
    : public TConfigurable
{
};

////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
