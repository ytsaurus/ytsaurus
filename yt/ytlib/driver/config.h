#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/meta_state/config.h>
#include <ytlib/transaction_client/config.h>
#include <ytlib/file_client/config.h>
#include <ytlib/table_client/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TDriverConfig
    : public TYsonSerializable
{
    NMetaState::TMasterDiscoveryConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NFileClient::TFileReaderConfigPtr FileReader;
    NFileClient::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
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
