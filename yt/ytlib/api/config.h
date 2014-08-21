#pragma once

#include "public.h"

#include <core/misc/config.h>

#include <core/ytree/yson_serializable.h>

#include <core/compression/public.h>

#include <ytlib/hydra/config.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/file_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/hive/config.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NHydra::TPeerConnectionConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    TMasterConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TMasterConnectionConfigPtr Master;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    TMasterConnectionConfigPtr MasterCache;
    NHive::TCellDirectoryConfigPtr CellDirectory;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    TSlruCacheConfigPtr CompressedBlockCache;
    NTabletClient::TTableMountCacheConfigPtr TableMountCache;
    
    TDuration QueryTimeout;
    
    NCompression::ECodec WriteRequestCodec;
    NCompression::ECodec LookupRequestCodec;
    
    int MaxRowsPerRead;
    int MaxRowsPerWrite;

    int DefaultInputRowLimit;
    int DefaultOutputRowLimit;

    TConnectionConfig();

};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public NChunkClient::TMultiChunkReaderConfig
{ };

DEFINE_REFCOUNTED_TYPE(TFileReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NFileClient::TFileChunkWriterConfig
{ };

DEFINE_REFCOUNTED_TYPE(TFileWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TJournalReaderConfig
    : public NChunkClient::TReplicationReaderConfig
{ };

DEFINE_REFCOUNTED_TYPE(TJournalReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TJournalWriterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxBatchDelay;
    i64 MaxBatchDataSize;
    int MaxBatchRowCount;
    bool PreferLocalHost;
    TDuration NodeRpcTimeout;
    TDuration NodePingPeriod;
    int MaxChunkOpenAttempts;
    int MaxChunkRowCount;
    i64 MaxChunkDataSize;
    TDuration NodeBanTimeout;

    TJournalWriterConfig()
    {
        RegisterParameter("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        RegisterParameter("max_batch_data_size", MaxBatchDataSize)
            .Default((i64) 16 * 1024 * 1024);
        RegisterParameter("max_batch_row_count", MaxBatchRowCount)
            .Default(100000);
        RegisterParameter("prefer_local_host", PreferLocalHost)
            .Default(true);
        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("node_ping_period", NodePingPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("max_chunk_open_attempts", MaxChunkOpenAttempts)
            .GreaterThan(0)
            .Default(5);
        RegisterParameter("max_chunk_row_count", MaxChunkRowCount)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("max_chunk_data_size", MaxChunkDataSize)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);
        RegisterParameter("node_ban_timeout", NodeBanTimeout)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TJournalWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

