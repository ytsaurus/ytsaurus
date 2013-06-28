#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/meta_state/config.h>

#include <ytlib/transaction_client/config.h>

#include <ytlib/file_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/rpc/retrying_channel.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDriverConfig
    : public TYsonSerializable
{
public:
    NMetaState::TMasterDiscoveryConfigPtr Masters;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NFileClient::TFileReaderConfigPtr FileReader;
    NFileClient::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;
    bool ReadFromFollowers;
    i64 ReadBufferSize;

    TDriverConfig()
    {
        RegisterParameter("masters", Masters);
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
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
        RegisterParameter("read_buffer_size", ReadBufferSize)
            .Default((i64) 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
