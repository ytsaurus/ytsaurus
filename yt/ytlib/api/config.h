#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/compression/public.h>

#include <ytlib/hydra/config.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/file_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/chunk_client/config.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/tablet_client/public.h>

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
    NChunkClient::TClientBlockCacheConfigPtr BlockCache;
    NTabletClient::TTableMountCacheConfigPtr TableMountCache;
    TDuration QueryTimeout;
    NCompression::ECodec WriteRequestCodec;
    NCompression::ECodec LookupRequestCodec;

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
    : public TYsonSerializable
{ };

DEFINE_REFCOUNTED_TYPE(TJournalReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TJournalWriterConfig
    : public TYsonSerializable
{
public:
    TDuration MaxBatchDelay;
    i64 MaxBatchDataSize;
    int MaxBatchRecordCount;
    bool PreferLocalHost;
    TDuration NodeRpcTimeout;
    TDuration NodePingPeriod;

    TJournalWriterConfig()
    {
        RegisterParameter("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        RegisterParameter("max_batch_data_size", MaxBatchDataSize)
            .Default((i64) 16 * 1024 * 1024);
        RegisterParameter("max_batch_record_count", MaxBatchRecordCount)
            .Default(100000);
        RegisterParameter("prefer_local_host", PreferLocalHost)
            .Default(true);
        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("node_ping_period", NodeRpcTimeout)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TJournalWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

