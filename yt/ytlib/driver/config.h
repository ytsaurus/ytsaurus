#pragma once

#include "public.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDriverConfig
    : public TConfigurable
{
    NYTree::EYsonFormat OutputFormat;
    TDuration OperationWaitTimeout;
    NElection::TLeaderLookup::TConfigPtr Masters;
    NTransactionClient::TTransactionManager::TConfig::TPtr TransactionManager;
    NFileClient::TFileReader::TConfig::TPtr FileReader;
    NFileClient::TFileWriter::TConfig::TPtr FileWriter;
    NTableClient::TTableReader::TConfig::TPtr TableReader;
    NTableClient::TTableWriter::TConfig::TPtr TableWriter;
    NChunkClient::TClientBlockCacheConfig::TPtr BlockCache;

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
