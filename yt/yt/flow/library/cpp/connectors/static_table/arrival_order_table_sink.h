#pragma once

#include "public.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <deque>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

//! Identifies the writer that owns an output directory's progress.
struct TArrivalOrderTableSinkOwner
    : public NYTree::TYsonStruct
{
    NYPath::TRichYPath PipelinePath;
    TComputationId ComputationId;
    TSinkId SinkId;

    REGISTER_YSON_STRUCT(TArrivalOrderTableSinkOwner);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArrivalOrderTableSinkOwner);

struct TArrivalOrderTableSinkPartitionProgress
    : public NYTree::TYsonStruct
{
    TSystemTimestamp SystemTimestamp;
    TMessageId MessageId;

    REGISTER_YSON_STRUCT(TArrivalOrderTableSinkPartitionProgress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArrivalOrderTableSinkPartitionProgress);

struct TArrivalOrderTableSinkProgress
    : public NYTree::TYsonStruct
{
    TArrivalOrderTableSinkOwnerPtr Owner;
    THashMap<std::string, TArrivalOrderTableSinkPartitionProgressPtr> Partitions;
    TInstant NextTableTimestamp;

    REGISTER_YSON_STRUCT(TArrivalOrderTableSinkProgress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArrivalOrderTableSinkProgress);

////////////////////////////////////////////////////////////////////////////////

class TArrivalOrderTableSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TArrivalOrderTableSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicArrivalOrderTableSinkParameters);

    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override;
};

////////////////////////////////////////////////////////////////////////////////

class TArrivalOrderTableSink
    : public TSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TArrivalOrderTableSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicArrivalOrderTableSinkParameters);

    using TSinkController = TArrivalOrderTableSinkController;

    TArrivalOrderTableSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;
    void UpdateWatermarkState(TWatermarkStatePtr state) override;

private:
    struct TRequest
    {
        TOutputMessageConstPtr Message;
        TOnDistributedCallback Callback;
        i64 DataWeight{};
    };

    struct TBatch
        : public TRefCounted
    {
        TInstant TableTimestamp;
        std::vector<TRequest> Requests;
    };

    using TBatchPtr = TIntrusivePtr<TBatch>;

    NApi::IClientPtr Client_;
    NTableClient::TTableSchemaPtr Schema_;
    IStatusErrorStatePtr RetryErrorState_;
    NYPath::TYPath OutputDirectory_;
    std::string PartitionKey_;
    std::optional<int> DataWeightColumnId_;
    TWatermarkStatePtr WatermarkState_;

    bool Initialized_ = false;
    std::optional<TMessageId> PersistedThroughMessageId_;
    TInstant NextTableTimestamp_;
    i64 ActiveDataWeight_ = 0;
    std::vector<TRequest> ActiveRequests_;
    std::deque<TRequest> DeferredRequests_;
    std::deque<TBatchPtr> ReadyBatches_;
    TArrivalOrderTableSinkProgressPtr InFlightProgress_;
    TBatchPtr InFlightBatch_;

    static TInstant GetNextTableTimestamp(TInstant now, TDuration tablePeriod);
    TArrivalOrderTableSinkPartitionProgressPtr GetPartitionProgress(const TArrivalOrderTableSinkProgressPtr& progress) const;
    static std::optional<TMessageId> GetMessageId(const TArrivalOrderTableSinkPartitionProgressPtr& partitionProgress);
    TArrivalOrderTableSinkProgressPtr ReadOrSeedProgress(const NApi::ITransactionPtr& transaction) const;
    TArrivalOrderTableSinkOwnerPtr GetOwner() const;
    void ValidateProgressOwnership(const TArrivalOrderTableSinkProgressPtr& progress) const;
    void LockProgress(const NApi::ITransactionPtr& transaction) const;
    TSystemTimestamp GetSystemWatermark() const;
    bool IsActiveBatchFull() const;

    template <class TCallback>
    TArrivalOrderTableSinkProgressPtr RunWithRetries(TCallback&& callback, TStringBuf operation);

    void RouteRequest(TRequest request);
    void AddRequest(TRequest request);
    bool IsEmptySlotReady(TInstant now) const;
    void AssignDeferredRequests(TInstant now);
    void SealActiveBatch();
    void EnsureInitialized();
    TArrivalOrderTableSinkProgressPtr InitializeExternalState();
    TArrivalOrderTableSinkProgressPtr CommitBatchOnce(const TBatchPtr& batch, const TDynamicArrivalOrderTableSinkParametersPtr& dynamicParameters, TSystemTimestamp systemWatermark);
    NApi::ITransactionPtr StartTransaction(const TDynamicArrivalOrderTableSinkParametersPtr& dynamicParameters) const;
    i64 GetDataWeight(const TOutputMessageConstPtr& message) const;
};

DECLARE_REFCOUNTED_TYPE(TArrivalOrderTableSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
