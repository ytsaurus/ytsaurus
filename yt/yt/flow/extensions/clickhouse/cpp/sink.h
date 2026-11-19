#pragma once

#include "public.h"

#include "block_builder.h"
#include "describe_traits.h"
#include "shard.h"
#include "spec.h"

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
);

DEFINE_ENUM(EClickHouseErrorKind,
    (Retryable)
    (Unclassified)
    (Permanent)
);

EClickHouseErrorKind ClassifyClickHouseError(const std::exception& ex);

DEFINE_ENUM(EClickHouseFailureAction,
    (Retry)
    (Fail)
);

DEFINE_ENUM(EClickHouseShardErrorPhase,
    (Configuration)
    (Writing)
);

DEFINE_ENUM(EClickHouseAdmissionFailureKind,
    (Unavailable)
    (Indeterminate)
    (Incompatible)
);

DEFINE_ENUM(EClickHouseAdmissionPhase,
    (Connect)
    (QueryMetadata)
    (Validate)
    (QueryDedupWindow)
);

EClickHouseAdmissionFailureKind ClassifyClickHouseAdmissionError(const std::exception& ex);

class TClickHouseAdmissionException
    : public TErrorException
{
public:
    TClickHouseAdmissionException(EClickHouseAdmissionFailureKind kind, TError error);

    EClickHouseAdmissionFailureKind GetKind() const;

private:
    EClickHouseAdmissionFailureKind Kind_;
};

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
    EClickHouseErrorKind errorKind,
    const TClickHouseRequestOptions& requestOptions,
    TClickHouseAttemptState* attemptState);

////////////////////////////////////////////////////////////////////////////////

int SelectClickHouseCandidateStart(
    EClickHouseHostSelectionPolicy policy,
    int hostCount,
    int randomOffset);

clickhouse::ClientOptions MakeClientOptions(
    const TCommonClickHouseSinkParameters& parameters,
    const TClickHouseShard& shard,
    const std::string& host,
    TDuration socketTimeout);

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

void ValidateClickHouseEndpointMetadata(
    const TClickHouseShard& shard,
    const std::string& host,
    const TClickHouseTargetMetadata& endpoint,
    const TClickHouseTargetMetadata* reference,
    const NLogging::TLogger& logger);

void ValidateAndUpdateShardTargetIdentityFingerprints(
    TClickHouseShardTopologyState* state,
    const std::vector<std::string>& currentFingerprints,
    const std::deque<TMessageId>& pendingBatchBounds,
    bool isSharded);

TStringBuf GetClickHouseDedupWindowSettingName(bool asyncInsert);

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

DEFINE_ENUM(EClickHouseConnectionPurpose,
    (Probe)
    (Write)
);

struct IClickHouseConnection
{
    virtual ~IClickHouseConnection() = default;

    virtual TClickHouseTargetMetadata QueryTargetMetadata() = 0;
    virtual std::optional<TDuration> QueryDedupWindow(bool asyncInsert) = 0;
    virtual void Insert(const TClickHouseShardWrite& shardWrite, bool asyncInsert, bool useInputFunction) = 0;
};

struct TClickHouseWriterTestHooks
{
    std::function<std::unique_ptr<IClickHouseConnection>(
        int shardIndex,
        const std::string& host,
        EClickHouseConnectionPurpose purpose,
        TDuration socketTimeout)>
        CreateConnection;
    std::function<int(int hostCount)> SelectRandomStartOffset;
};

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

    std::vector<TResolvedColumn> Connect(const NTableClient::TTableSchemaPtr& streamSchema);
    void Reconfigure(TDynamicCommonClickHouseSinkParametersPtr dynamicParameters);

    const std::vector<TClickHouseShard>& GetShards() const;
    std::vector<std::string> GetShardTargetIdentityFingerprints() const;

    void Run(TWeakPtr<TRefCounted> owner);

    TFuture<void> Write(EWriteGuarantee guarantee, std::vector<TClickHouseShardWrite> shardWrites);

private:
    struct TWriteRequest
    {
        EWriteGuarantee Guarantee = EWriteGuarantee::ExactlyOnce;
        std::vector<TClickHouseShardWrite> ShardWrites;
        // Shards before it are already committed; a retry resumes here so an
        // already inserted shard is never written twice within one attempt.
        int NextShardIndex = 0;
        TPromise<void> Promise;
    };

    struct TShardConnection
    {
        std::unique_ptr<IClickHouseConnection> Connection;
        std::optional<int> HostIndex;
        i64 Generation = 0;
        std::optional<std::string> TargetEngine;
        std::optional<TDuration> TargetDedupWindow;
        bool RequiresAdmission = false;
    };

    struct TCandidateTraversal
    {
        bool InSequence = false;
        int NextHostIndex = 0;
        int VisitedHostCount = 0;
        std::vector<TError> UnavailableHostErrors;
        std::vector<std::string> IndeterminateHosts;
    };

    const TCommonClickHouseSinkParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicCommonClickHouseSinkParameters> DynamicParameters_;
    const std::vector<TClickHouseShard> Shards_;
    const IStatusErrorStatePtr ErrorState_;
    const NLogging::TLogger Logger;
    const std::optional<TClickHouseWriterTestHooks> TestHooks_;

    NConcurrency::TNonblockingQueue<TWriteRequest> Queue_;
    std::vector<TShardConnection> Connections_;
    std::vector<TCandidateTraversal> CandidateTraversals_;
    std::vector<TClickHouseTargetMetadata> ShardReferences_;
    i64 LastConnectionGeneration_ = 0;
    TDynamicCommonClickHouseSinkParametersPtr AppliedConnectionDynamicParameters_;
    bool RequiresInputFunction_ = false;
    std::map<int, TClickHouseShardErrors> ActiveShardErrors_;

    NLogging::TLogger MakeShardLogger(int shardIndex) const;
    bool UsesAsyncInsert(const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters) const;
    std::unique_ptr<IClickHouseConnection> CreateConnection(
        int shardIndex,
        int hostIndex,
        EClickHouseConnectionPurpose purpose,
        TDuration writeTimeout);
    TError MakeEndpointError(
        TError error,
        int shardIndex,
        int hostIndex,
        EClickHouseAdmissionPhase phase,
        EClickHouseAdmissionFailureKind kind,
        const std::exception& ex) const;
    void IntrospectEndpoints();
    int SelectPolicyCandidateStart(int shardIndex) const;
    int SelectFailoverCandidateStart(int shardIndex) const;
    void BeginCandidateSequence(int shardIndex, bool failover);
    void EndCandidateSequence(int shardIndex);
    void ResetCandidateSequences();
    TShardConnection AdmitCandidate(
        int shardIndex,
        int hostIndex,
        const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    TShardConnection AdmitNextCandidate(
        int shardIndex,
        const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    void ReadmitConnection(int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    void ApplyConnectionDynamicParameters(const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters);
    std::optional<TError> GetAdmissionTerminalError(
        const TClickHouseAdmissionException& ex,
        const TClickHouseRequestOptions& requestOptions,
        TClickHouseAttemptState* attemptState) const;
    void SetShardError(int shardIndex, EClickHouseShardErrorPhase phase, TError error);
    void ClearShardError(int shardIndex, EClickHouseShardErrorPhase phase);
    void PublishShardErrors();
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
        NTableClient::TTableSchemaPtr streamSchema,
        NLogging::TLogger logger);

    ~TCommonClickHouseSink() override;

protected:
    const NLogging::TLogger Logger;

    void EnsureSessionStarted();
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
    const NTableClient::TTableSchemaPtr StreamSchema_;

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
    const clickhouse::Block& block,
    bool asyncInsert,
    const std::optional<std::string>& dedupToken,
    bool useInputFunction = false);

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
