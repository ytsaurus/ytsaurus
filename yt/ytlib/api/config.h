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

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/bus/config.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EConnectionType,
    (Native)
    (Rpc)
);

////////////////////////////////////////////////////////////////////////////////

class TConnectionConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    EConnectionType ConnectionType;

    TConnectionConfig()
    {
        RegisterParameter("connection_type", ConnectionType)
            .Default(EConnectionType::Native);
    }
};

DEFINE_REFCOUNTED_TYPE(TConnectionConfig)

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

class TTableMountCacheConfig
    : public NTabletClient::TTableMountCacheConfig
{
public:
    int OnErrorRetryCount;
    TDuration OnErrorSlackPeriod;

    TTableMountCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TNativeConnectionConfig
    : public TConnectionConfig
    , public NChunkClient::TChunkTeleporterConfig
{
public:
    TNullable<NNodeTrackerClient::TNetworkPreferenceList> Networks;
    TMasterConnectionConfigPtr PrimaryMaster;
    std::vector<TMasterConnectionConfigPtr> SecondaryMasters;
    TMasterConnectionConfigPtr MasterCache;
    bool EnableReadFromFollowers;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveClient::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    NScheduler::TSchedulerConnectionConfigPtr Scheduler;
    NTransactionClient::TTransactionManagerConfigPtr TransactionManager;
    NChunkClient::TBlockCacheConfigPtr BlockCache;
    TTableMountCacheConfigPtr TableMountCache;
    NHiveClient::TClusterDirectorySynchronizerConfigPtr ClusterDirectorySynchronizer;

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

    TNullable<int> ThreadPoolSize;

    int MaxConcurrentRequests;

    NBus::TTcpBusConfigPtr BusClient;

    TNativeConnectionConfig();

};

DEFINE_REFCOUNTED_TYPE(TNativeConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
{ };

DEFINE_REFCOUNTED_TYPE(TFileReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NFileClient::TFileChunkWriterConfig
{
public:
    TDuration UploadTransactionTimeout;

    TFileWriterConfig()
    {
        RegisterParameter("upload_transaction_timeout", UploadTransactionTimeout)
            .Default(TDuration::Seconds(15));
    }
};

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

    NRpc::TRetryingChannelConfigPtr NodeChannel;

    TDuration UploadTransactionTimeout;

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

        RegisterParameter("node_channel", NodeChannel)
            .DefaultNew();

        RegisterParameter("upload_transaction_timeout", UploadTransactionTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TJournalWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TPersistentQueuePollerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Try to keep at most this many prefetched rows in memory. This limit is approximate.
    i64 MaxPrefetchRowCount;

    //! Try to keep at most this much prefetched data in memory. This limit is approximate.
    i64 MaxPrefetchDataWeight;

    //! The limit for the number of rows to be requested in a single background fetch request.
    i64 MaxRowsPerFetch;

    //! The limit for the number of rows to be returned by #TPersistentQueuePoller::Poll call.
    i64 MaxRowsPerPoll;

    //! When trimming data table, keep the number of consumed but untrimmed rows about this level.
    i64 UntrimmedDataRowsLow;

    //! When more than this many of consumed but untrimmed rows appear in data table, trim the front ones
    //! in accordance to #UntrimmedDataRowsLow.
    i64 UntrimmedDataRowsHigh;

    //! How often the data table is to be polled.
    TDuration DataPollPeriod;

    //! How often the state table is to be trimmed.
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
        RegisterParameter("untrimmed_data_rows_low", UntrimmedDataRowsLow)
            .Default(0);
        RegisterParameter("untrimmed_data_rows_high", UntrimmedDataRowsHigh)
            .Default(std::numeric_limits<i64>::max());
        RegisterParameter("data_poll_period", DataPollPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("state_trim_period", StateTrimPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("backoff_time", BackoffTime)
            .Default(TDuration::Seconds(5));

        RegisterPostprocessor([&] {
            if (UntrimmedDataRowsLow > UntrimmedDataRowsHigh) {
                THROW_ERROR_EXCEPTION("\"untrimmed_data_rows_low\" must not exceed \"untrimmed_data_rows_high\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TPersistentQueuePollerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

