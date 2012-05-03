#pragma once

#include "public.h"

#include <ytlib/election/leader_lookup.h>
// TODO: consider using forward declarations.
#include <ytlib/transaction_client/config.h>
#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>
#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDriverConfig
    : public TConfigurable
{
    NYTree::EYsonFormat OutputFormat;
    NElection::TLeaderLookup::TConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NFileClient::TFileReader::TConfig::TPtr FileReader;
    NFileClient::TFileWriter::TConfig::TPtr FileWriter;
    NTableClient::TTableReader::TConfig::TPtr TableReader;
    NTableClient::TTableWriter::TConfig::TPtr TableWriter;
    NChunkClient::TClientBlockCacheConfig::TPtr BlockCache;

    TDriverConfig()
    {
        Register("output_format", OutputFormat)
            .Default(NYTree::EYsonFormat::Text);
        Register("masters", Masters);
        Register("transaction_manager", TransactionManager)
            .DefaultNew();
        Register("file_reader", FileReader)
            .DefaultNew();
        Register("file_writer", FileWriter)
            .DefaultNew();
        Register("table_reader", TableReader)
            .DefaultNew();
        Register("table_writer", TableWriter)
            .DefaultNew();
        Register("block_cache", BlockCache)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
