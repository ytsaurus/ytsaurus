#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/meta_state/config.h>
#include <ytlib/transaction_client/config.h>
#include <ytlib/file_client/config.h>
#include <ytlib/table_client/config.h>
#include <ytlib/rpc/retrying_channel.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDriverConfig
    : public TYsonSerializable
{
public:
    NMetaState::TMasterDiscoveryConfigPtr Masters;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NFileClient::TFileReaderConfigPtr FileReader;
    NFileClient::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;
    bool ReadFromFollowers;

    TDriverConfig()
    {
        RegisterParameter("masters", Masters);
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("file_reader", FileReader)
            .DefaultNew();
        RegisterParameter("file_writer", FileWriter)
            .DefaultNew();
        RegisterParameter("table_reader", TableReader)
            .DefaultNew();
        RegisterParameter("table_writer", TableWriter)
            .DefaultNew();
        RegisterParameter("block_cache", BlockCache)
            .DefaultNew();
        RegisterParameter("read_from_followers", ReadFromFollowers)
            .Describe("Enable read-only requests to followers")
            .Default(false);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
