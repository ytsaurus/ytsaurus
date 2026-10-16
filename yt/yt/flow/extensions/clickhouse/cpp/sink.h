#pragma once

#include "public.h"

#include "block_builder.h"
#include "describe_traits.h"
#include "shard.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/block.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/client.h>

#include <deque>
#include <functional>
#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWriteGuarantee,
    (ExactlyOnce)
    (AtLeastOnce)
    (AtMostOnce)
);

DEFINE_ENUM(EClickHouseErrorKind,
    (Retryable)
    (Unclassified)
    (Permanent)
);

EClickHouseErrorKind ClassifyClickHouseError(const std::exception& ex);

DEFINE_ENUM(EClickHouseFailureAction,
    (Retry)
    (Acknowledge)
    (Fail)
);

DEFINE_ENUM(EClickHouseShardErrorPhase,
    (Configuration)
    (Writing)
);

struct TClickHouseRequestOptions
{
    bool AsyncInsert = false;
    i64 MaxInsertAttempts = 0;
};

struct TClickHouseAttemptState
{
    i64 UnclassifiedInsertAttempts = 0;
};

TClickHouseRequestOptions MakeClickHouseRequestOptions(
    const TDynamicCommonClickHouseSinkParameters& dynamicParameters);

EClickHouseFailureAction GetClickHouseFailureAction(
    EWriteGuarantee guarantee,
    bool insertStarted,
    EClickHouseErrorKind errorKind,
    const TClickHouseRequestOptions& requestOptions,
    TClickHouseAttemptState* attemptState);

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> OrderHostsForClient(
    const std::vector<std::string>& hosts,
    EClickHouseHostSelectionPolicy policy,
    size_t startOffset);

clickhouse::ClientOptions MakeClientOptions(
    const TCommonClickHouseSinkParameters& parameters,
    const TClickHouseShard& shard,
    TDuration writeTimeout);

clickhouse::ClientOptions MakeClientOptions(
    const TCommonClickHouseSinkParameters& parameters,
    const TClickHouseShard& shard,
    TDuration writeTimeout,
    size_t startOffset);

NLogging::TLogger MakeSinkLogger(
    const NLogging::TLogger& logger,
    const TCommonClickHouseSinkParameters& parameters);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseTargetMetadata
{
    std::string Engine;
    std::vector<TClickHouseTableColumn> Columns;
    std::optional<TClickHouseReplicationIdentity> ReplicationIdentity;
    std::string ReplicaName;
};

struct TClickHouseShardErrors
{
    std::optional<TError> Configuration;
    std::optional<TError> Writing;
};

bool ShouldConsumeClickHouseMetadataBlock(
    size_t actualColumnCount,
    size_t rowCount,
    size_t expectedColumnCount);

void ValidateFailoverMetadata(
    const TClickHouseShard& shard,
    const std::vector<TClickHouseTargetMetadata>& endpoints,
    const NLogging::TLogger& logger);

void ValidateAndUpdateShardTargetIdentityFingerprints(
    TClickHouseShardTopologyState* state,
    const std::vector<std::string>& currentFingerprints,
    const std::deque<TMessageId>& pendingBatchBounds,
    bool isSharded);

TStringBuf GetClickHouseDedupWindowSettingName(bool asyncInsert);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseWriterTestHooks
{
    std::function<TClickHouseTargetMetadata(int shardIndex, const std::string& host)> QueryEndpointMetadata;
    std::function<void(int shardIndex)> ResetConnection;
    std::function<void(int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr&)> ApplyConnectionDynamicParameters;
    std::function<void(int shardIndex, bool asyncInsert)> Insert;
};

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseShardWrite
{
    int ShardIndex = 0;
    std::optional<std::string> DedupToken;
    clickhouse::Block Block;
};

std::vector<TClickHouseShardWrite> BuildClickHouseShardWrites(
    const TClickHouseShardRouter& router,
    const TClickHouseBlockBuilder& blockBuilder,
    const std::optional<std::string>& batchDedupToken,
    const std::vector<TOutputMessageConstPtr>& messages);

////////////////////////////////////////////////////////////////////////////////

class TClickHouseWriter
    : public TRefCounted
{
public:
    TClickHouseWriter(
        TCommonClickHouseSinkParametersPtr parameters,
        TDynamicCommonClickHouseSinkParametersPtr dynamicParameters,
        std::vector<TClickHouseShard> shards,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger,
        std::optional<TClickHouseWriterTestHooks> testHooks = std::nullopt);

    void Connect();
    void Reconfigure(TDynamicCommonClickHouseSinkParametersPtr dynamicParameters);

    const std::vector<TClickHouseShard>& GetShards() const;
    std::vector<std::string> GetShardTargetIdentityFingerprints() const;
    const std::vector<TClickHouseTableColumn>& GetTableColumns() const;
    void SetResolvedColumns(std::vector<TResolvedColumn> columns);

    void Run(TWeakPtr<TRefCounted> owner);

    TFuture<void> Write(EWriteGuarantee guarantee, std::vector<TClickHouseShardWrite> shardWrites);

private:
    struct TWriteRequest
    {
        EWriteGuarantee Guarantee = EWriteGuarantee::ExactlyOnce;
        std::vector<TClickHouseShardWrite> ShardWrites;
        // Shards before it are already committed or abandoned; a retry resumes here so an
        // already inserted shard is never written twice within one attempt.
        int NextShardIndex = 0;
        TPromise<void> Promise;
    };

    struct TShardConnection
    {
        std::unique_ptr<clickhouse::Client> Client;
        std::optional<std::string> TargetEngine;
        std::optional<TDuration> TargetDedupWindow;
        bool ReconnectBeforeInsert = false;
    };

    const TCommonClickHouseSinkParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicCommonClickHouseSinkParameters> DynamicParameters_;
    const std::vector<TClickHouseShard> Shards_;
    const IStatusErrorStatePtr ErrorState_;
    const NLogging::TLogger Logger;
    const std::optional<TClickHouseWriterTestHooks> TestHooks_;

    NConcurrency::TNonblockingQueue<TWriteRequest> Queue_;
    std::vector<TShardConnection> Connections_;
    std::vector<std::vector<TClickHouseTargetMetadata>> EndpointMetadata_;
    TDynamicCommonClickHouseSinkParametersPtr AppliedConnectionDynamicParameters_;
    std::vector<TClickHouseTableColumn> TableColumns_;
    std::vector<TResolvedColumn> ResolvedColumns_;
    std::map<int, TClickHouseShardErrors> ActiveShardErrors_;

    NLogging::TLogger MakeShardLogger(int shardIndex) const;
    TClickHouseTargetMetadata QueryEndpointMetadata(
        int shardIndex,
        const std::string& host,
        const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    void IntrospectEndpoints(
        const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    std::optional<TDuration> QueryDedupWindow(int shardIndex, bool asyncInsert);
    void ApplyConnectionDynamicParameters(const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    void ValidateTarget(int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    void SetShardError(int shardIndex, EClickHouseShardErrorPhase phase, TError error);
    void ClearShardError(int shardIndex, EClickHouseShardErrorPhase phase);
    void PublishShardErrors();
    void ResetConnection(int shardIndex);
    void Insert(const TWriteRequest& request, int shardWriteIndex, bool asyncInsert);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseWriter);

////////////////////////////////////////////////////////////////////////////////

class TCommonClickHouseSink
    : public virtual TRefCounted
{
public:
    TCommonClickHouseSink(
        TCommonClickHouseSinkParametersPtr parameters,
        TDynamicCommonClickHouseSinkParametersPtr dynamicParameters,
        IStatusProfilerPtr statusProfiler,
        std::vector<NTableClient::TTableSchemaPtr> streamSchemas,
        NLogging::TLogger logger);

    ~TCommonClickHouseSink() override;

protected:
    const NLogging::TLogger Logger;

    void EnsureSessionStarted();
    void EnsureSessionStartedWithRetry();
    TIntrusivePtr<TRefCounted> GetSessionLifetime() const;
    std::vector<std::string> GetShardTargetIdentityFingerprints() const;

    TFuture<void> WriteMessages(
        EWriteGuarantee guarantee,
        const std::optional<std::string>& batchDedupToken,
        const std::vector<TOutputMessageConstPtr>& messages);
    void Reconfigure(TDynamicCommonClickHouseSinkParametersPtr dynamicParameters);

private:
    class TWriterSession;

    TError TryStartSession();

    const TCommonClickHouseSinkParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicCommonClickHouseSinkParameters> DynamicParameters_;
    const IStatusErrorStatePtr ErrorState_;
    const std::vector<NTableClient::TTableSchemaPtr> StreamSchemas_;

    std::optional<TClickHouseBlockBuilder> BlockBuilder_;
    std::optional<TClickHouseShardRouter> Router_;
    TIntrusivePtr<TWriterSession> Session_;
};

////////////////////////////////////////////////////////////////////////////////

class TClickHouseBatchingSinkBase
    : public TOrderedBatchingAsyncSinkBase
    , public TCommonClickHouseSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TClickHouseBatchingSinkBaseParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicClickHouseBatchingSinkParameters);

    using TSinkController = TClickHouseSinkController;
    using TDescribeTraits = TClickHouseDescribeTraits;

    TClickHouseBatchingSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;

private:
    using TCommonClickHouseSink::Logger;

    TMutableStateClient<TClickHouseShardTopologyState> TopologyState_;
    bool TargetIdentityValidated_ = false;

    //! Refuses to start when the shard topology changed while batches cut under the previous
    //! one are still undelivered: replaying them would reroute rows away from the shard whose
    //! deduplication token they already consumed.
    void ValidateShardTopologyUnchanged();
    void ValidateShardTargetIdentityUnchanged();

    void DoInit(const std::string& producerId) final;
    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo) final;
};

DEFINE_REFCOUNTED_TYPE(TClickHouseBatchingSinkBase);

class TClickHouseBatchingSink
    : public TClickHouseBatchingSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TClickHouseBatchingSinkParameters);
    using TClickHouseBatchingSinkBase::TClickHouseBatchingSinkBase;
};

DEFINE_REFCOUNTED_TYPE(TClickHouseBatchingSink);

class TShardedClickHouseBatchingSink
    : public TClickHouseBatchingSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TShardedClickHouseBatchingSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicShardedClickHouseBatchingSinkParameters);
    using TClickHouseBatchingSinkBase::TClickHouseBatchingSinkBase;
};

DEFINE_REFCOUNTED_TYPE(TShardedClickHouseBatchingSink);

////////////////////////////////////////////////////////////////////////////////

class TAtLeastOnceClickHouseSink
    : public TSyncSinkBase
    , public TCommonClickHouseSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAtLeastOnceClickHouseSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAtLeastOnceClickHouseSinkParameters);

    using TSinkController = TClickHouseSinkController;
    using TDescribeTraits = TClickHouseDescribeTraits;

    TAtLeastOnceClickHouseSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    using TCommonClickHouseSink::Logger;

    void DoInit() final;
    void DoDistribute(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::deque<TOutputMessageConstPtr>& messages) final;
};

DEFINE_REFCOUNTED_TYPE(TAtLeastOnceClickHouseSink);

////////////////////////////////////////////////////////////////////////////////

class TAtMostOnceClickHouseSink
    : public TDelegatingAsyncSinkBase
    , public TCommonClickHouseSink
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAtMostOnceClickHouseSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAtMostOnceClickHouseSinkParameters);

    using TSinkController = TClickHouseSinkController;
    using TDescribeTraits = TClickHouseDescribeTraits;

    TAtMostOnceClickHouseSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    ~TAtMostOnceClickHouseSink() override;

    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;

private:
    using TCommonClickHouseSink::Logger;

    bool IsAtMostOnceStrategyEnabled() const;

    void DoInit(const std::string& producerId) final;
    std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) final;
};

DEFINE_REFCOUNTED_TYPE(TAtMostOnceClickHouseSink);

////////////////////////////////////////////////////////////////////////////////

class TClickHouseSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TClickHouseSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicClickHouseSinkControllerParameters);

    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() final;
};

DEFINE_REFCOUNTED_TYPE(TClickHouseSinkController);

////////////////////////////////////////////////////////////////////////////////

std::string BuildDedupToken(const std::vector<TOutputMessageConstPtr>& messages);

std::string BuildInsertHeader(
    const std::string& database,
    const std::string& table,
    const std::vector<TResolvedColumn>& columns,
    bool asyncInsert,
    const std::optional<std::string>& dedupToken);

bool IsBlockDeduplicatingEngine(const std::string& engine);

bool IsPlainMergeTreeEngine(const std::string& engine);

void ValidateTargetEngine(
    const std::string& engine,
    const std::string& database,
    const std::string& table,
    bool hasFailoverHosts,
    const NLogging::TLogger& Logger);

void WarnIfDedupWindowBelowReplayHorizon(
    std::optional<TDuration> dedupWindow,
    TDuration replayHorizon,
    TStringBuf dedupWindowSettingName,
    const std::string& database,
    const std::string& table,
    const NLogging::TLogger& Logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
