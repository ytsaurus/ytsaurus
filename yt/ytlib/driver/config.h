#pragma once

#include "public.h"

#include <ytlib/election/leader_lookup.h>
// TODO: consider using forward declarations.
#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/config.h>
#include <ytlib/file_client/public.h>
#include <ytlib/file_client/config.h>
#include <ytlib/table_client/public.h>
#include <ytlib/table_client/config.h>
#include <ytlib/chunk_client/client_block_cache.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDriverConfig
    : public TConfigurable
{
    NYTree::EYsonFormat OutputFormat;
    TDuration OperationWaitTimeout;
    NElection::TLeaderLookup::TConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NFileClient::TFileReaderConfigPtr FileReader;
    NFileClient::TFileWriterConfigPtr FileWriter;
    NTableClient::TChunkSequenceReaderConfigPtr ChunkSequenceReader;
    NTableClient::TChunkSequenceWriterConfigPtr ChunkSequenceWriter;
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;

    TDriverConfig()
    {
        Register("output_format", OutputFormat)
            .Default(NYTree::EYsonFormat::Text);
        Register("operation_wait_timeout", OperationWaitTimeout)
            .Default(TDuration::Seconds(3));
        Register("masters", Masters);
        Register("transaction_manager", TransactionManager)
            .DefaultNew();
        Register("file_reader", FileReader)
            .DefaultNew();
        Register("file_writer", FileWriter)
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

} // namespace NDriver
} // namespace NYT
