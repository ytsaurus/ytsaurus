#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/election/leader_lookup.h>
#include <ytlib/transaction_client/config.h>
#include <ytlib/file_client/config.h>
#include <ytlib/table_client/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDriverConfig
    : public TConfigurable
{
    NElection::TLeaderLookup::TConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NFileClient::TFileReaderConfigPtr FileReader;
    NFileClient::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableConsumerConfigPtr TableConsumer;
    NTableClient::TChunkSequenceReaderConfigPtr ChunkSequenceReader;
    NTableClient::TChunkSequenceWriterConfigPtr ChunkSequenceWriter;
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;

    TDriverConfig()
    {
        Register("masters", Masters);
        Register("transaction_manager", TransactionManager)
            .DefaultNew();
        Register("file_reader", FileReader)
            .DefaultNew();
        Register("file_writer", FileWriter)
            .DefaultNew();
        Register("table_consumer", TableConsumer)
            .DefaultNew();
        Register("chunk_sequence_reader", ChunkSequenceReader)
            .DefaultNew();
        Register("chunk_sequence_writer", ChunkSequenceWriter)
            .DefaultNew();
        Register("block_cache", BlockCache)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDsvFormatConfig
    : public TConfigurable
{
    char NewLineSeparator;
    char KeyValueSeparator;
    char ItemSeparator;

    TDsvFormatConfig()
    {
        Register("newline", NewLineSeparator).Default('\n');
        Register("key_value", KeyValueSeparator).Default('=');
        Register("item", ItemSeparator).Default('\t');
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
