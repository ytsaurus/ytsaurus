#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/retrying_channel.h>

#include <ytlib/hydra/config.h>

#include <ytlib/transaction_client/config.h>

#include <ytlib/file_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/hive/config.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public TYsonSerializable
{
public:
    NHydra::TPeerDiscoveryConfigPtr Masters;
    NHive::TRemoteTimestampProviderConfigPtr TimestampProvider;
    NHive::TCellDirectoryConfigPtr CellDirectory;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;
    NTabletClient::TTableMountCacheConfigPtr TableMountCache;

    TConnectionConfig()
    {
        RegisterParameter("masters", Masters);
        RegisterParameter("timestamp_provider", TimestampProvider);
        RegisterParameter("cell_directory", CellDirectory)
            .DefaultNew();
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("block_cache", BlockCache)
            .DefaultNew();
        RegisterParameter("table_mount_cache", TableMountCache)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{ };

////////////////////////////////////////////////////////////////////////////////

class TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NFileClient::TFileChunkWriterConfig
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

