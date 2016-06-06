#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/file_client/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/hydra/config.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/query_client/config.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NHydra::TPeerConnectionConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    TMasterConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public NChunkClient::TChunkTeleporterConfig
{
public:
    Stroka NetworkName;
    TMasterConnectionConfigPtr PrimaryMaster;
    std::vector<TMasterConnectionConfigPtr> SecondaryMasters;
    TMasterConnectionConfigPtr MasterCache;
    bool EnableReadFromFollowers;
    bool ForceReadFromFollowers;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
    NTabletClient::TTableMountCacheConfigPtr TableMountCache;

    NQueryClient::TExecutorConfigPtr QueryEvaluator;
    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;
    TDuration QueryTimeout;
    NCompression::ECodec QueryResponseCodec;
    i64 DefaultInputRowLimit;
    i64 DefaultOutputRowLimit;

    TDuration WriteTimeout;
    NCompression::ECodec WriteRequestCodec;
    int MaxRowsPerWriteRequest;
    int MaxRowsPerTransaction;

    TDuration LookupTimeout;
    NCompression::ECodec LookupRequestCodec;
    NCompression::ECodec LookupResponseCodec;
    int MaxRowsPerReadRequest;

    bool EnableUdf;
    NYPath::TYPath UdfRegistryPath;
    TExpiringCacheConfigPtr FunctionRegistryCache;
    TSlruCacheConfigPtr FunctionImplCache;

    int TableMountInfoUpdateRetryCount;
    TDuration TableMountInfoUpdateRetryPeriod;

    int LightPoolSize;
    int HeavyPoolSize;

    TConnectionConfig();

};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
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
    , public TWorkloadConfig
{ };

DEFINE_REFCOUNTED_TYPE(TJournalReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TJournalWriterConfig
    : public virtual TWorkloadConfig
{
public:
    TDuration MaxBatchDelay;
    i64 MaxBatchDataSize;
    int MaxBatchRowCount;

    int MaxFlushRowCount;
    i64 MaxFlushDataSize;

    bool PreferLocalHost;

    TDuration NodeRpcTimeout;
    TDuration NodePingPeriod;
    TDuration NodeBanTimeout;

    int MaxChunkOpenAttempts;
    int MaxChunkRowCount;
    i64 MaxChunkDataSize;
    TDuration MaxChunkSessionDuration;

    TJournalWriterConfig()
    {
        RegisterParameter("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        RegisterParameter("max_batch_data_size", MaxBatchDataSize)
            .Default((i64) 16 * 1024 * 1024);
        RegisterParameter("max_batch_row_count", MaxBatchRowCount)
            .Default(100000);

        RegisterParameter("max_flush_row_count", MaxFlushRowCount)
            .Default(100000);
        RegisterParameter("max_flush_data_size", MaxFlushDataSize)
            .Default((i64) 100 * 1024 * 1024);

        RegisterParameter("prefer_local_host", PreferLocalHost)
            .Default(true);

        RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("node_ping_period", NodePingPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("node_ban_timeout", NodeBanTimeout)
            .Default(TDuration::Seconds(60));

        RegisterParameter("max_chunk_open_attempts", MaxChunkOpenAttempts)
            .GreaterThan(0)
            .Default(5);
        RegisterParameter("max_chunk_row_count", MaxChunkRowCount)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("max_chunk_data_size", MaxChunkDataSize)
            .GreaterThan(0)
            .Default((i64) 256 * 1024 * 1024);
        RegisterParameter("max_chunk_session_duration", MaxChunkSessionDuration)
            .Default(TDuration::Minutes(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TJournalWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TPersistentQueuePollerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Try to keep at most this many prefetched rows in memory. This limit is approximate.
    int MaxPrefetchRowCount;

    //! Try to keep at most this much prefetched data in memory. This limit is approximate.
    i64 MaxPrefetchDataWeight;

    //! The limit for the number of rows to be requested in a single background fetch request.
    int MaxRowsPerFetch;

    //! The limit for the number of rows to be returned by #TPersistentQueuePoller::Poll call.
    int MaxRowsPerPoll;

    //! How often the data table is to be polled.
    TDuration DataPollPeriod;

    //! How often the satte table is to be trimmed.
    TDuration StateTrimPeriod;

    //! For how long to backoff when a state conflict is detected.
    TDuration BackoffTime;

    TPersistentQueuePollerConfig()
    {
        RegisterParameter("max_prefetch_row_count", MaxPrefetchRowCount)
            .GreaterThan(0)
            .Default(1024);
        RegisterParameter("max_prefetch_data_weight", MaxPrefetchDataWeight)
            .GreaterThan(0)
            .Default((i64) 16 * 1024 * 1024);
        RegisterParameter("max_rows_per_fetch", MaxRowsPerFetch)
            .GreaterThan(0)
            .Default(512);
        RegisterParameter("max_rows_per_poll", MaxRowsPerPoll)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("data_poll_period", DataPollPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("state_trim_period", StateTrimPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("backoff_time", BackoffTime)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TPersistentQueuePollerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

