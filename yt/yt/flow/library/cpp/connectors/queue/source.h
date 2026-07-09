#pragma once

#include "public.h"

#include "queue_info.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/message_batcher.h>

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/queue_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TKey GenerateQueueKey(
    std::string_view sourceIdentity,
    int partitionIndex);
int ExtractQueuePartitionIndex(const TKey& key);

////////////////////////////////////////////////////////////////////////////////

class TQueueSourceImpl
    : public TIntegerOffsetOrderedSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TQueueSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicQueueSourceParameters);

    using TSourceController = TQueueSourceController;

    TQueueSourceImpl(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

private:
    void DoInit() final;
    void DoTerminate() final;

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimit) final;

    std::vector<TRecord> ParseData(const NQueueClient::IQueueRowsetPtr& rowset, i64 initialOffset, std::optional<i64> offsetLimit);
    virtual std::pair<std::vector<TPayload>, NTableClient::TTableSchemaPtr> UnpackRow(const NTableClient::TUnversionedRow& row, const NTableClient::TTableSchemaPtr& schema) = 0;

    void DoReportPersistedOffset(TOffset offsetExclusive) final;

    void TryUpdatePartitionInfo();

    //! Resolves the queue object type, caching it (the type never changes).
    NObjectClient::EObjectType GetQueueObjectType();

    bool IsReplicatedTableQueue();

    struct TSyncReplica
    {
        NApi::IClientPtr Client;
        NYPath::TYPath Path;
    };

    struct TTrimInfo
    {
        i64 TrimmedRowCount = 0;
        i64 TotalRowCount = 0;
    };

    //! Resolves any enabled synchronous replica from the replicated table's @replicas attribute.
    TSyncReplica ResolveSyncReplica();

    //! Fetches the trim point and total row count from a synchronous replica for a plain replicated table (its meta
    //! tablet info can run ahead of the read replica), or from the queue object itself otherwise.
    TTrimInfo FetchTrimInfo();

private:
    const int PartitionIndex_;

protected:
    const NLogging::TLogger Logger;

private:
    const NApi::IClientPtr ConsumerClient_;
    const NApi::IClientPtr QueueClient_;
    const NQueueClient::ISubConsumerClientPtr SubConsumerClient_;
    NConcurrency::TPeriodicExecutorPtr PartitionInfoUpdater_;
    std::atomic<i64> PersistedOffsetExclusive_ = 0;

    // Field for heuristic of fast committing last offset in case of finite source.
    std::atomic<i64> CommittedOffsetExclusive_ = 0;
    std::atomic<i64> MaxOffsetExclusive_ = 0;

    TFuture<std::vector<TRecord>> CurrentRequestFuture_;
    std::pair<TMessageBatcherSettingsPtr, i64> CurrentRequestParameters_;

    IStatusErrorStatePtr UpdatePartitionInfoErrorState_;

    TPromise<i64> InitialCommittedOffset_;

    // Cached queue object type, resolved once. Only accessed from the serialized invoker.
    std::optional<NObjectClient::EObjectType> QueueObjectType_;
};

////////////////////////////////////////////////////////////////////////////////

class TQueueSource
    : public TQueueSourceImpl
{
public:
    using TQueueSourceImpl::TQueueSourceImpl;

    std::pair<std::vector<TPayload>, NTableClient::TTableSchemaPtr> UnpackRow(const NTableClient::TUnversionedRow& row, const NTableClient::TTableSchemaPtr& schema) final;
};

DEFINE_REFCOUNTED_TYPE(TQueueSource);

////////////////////////////////////////////////////////////////////////////////

class TQueueSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TQueueSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicQueueSourceParameters);

    TQueueSourceController(
        TSourceControllerContextPtr context,
        TDynamicSourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;

    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override;

    std::string GetSourceIdentity() const override;

private:
    const TQueueInfoControllerPtr Info_;
};

DEFINE_REFCOUNTED_TYPE(TQueueSourceController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
