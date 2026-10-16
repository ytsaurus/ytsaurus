#pragma once

#include "joiner.h"
#include "public.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TServiceLogControllerState
    : public NYTree::TYsonStruct
{
    THashMap<TGuid, TServiceLogRangePtr> Ranges;
    THashSet<TServiceLogRangePtr> StrayRanges; // Its values are no longer in use, kept for reverse compatibility.
    std::optional<i64> CachedPartitionCount;
    bool WasPreviouslyFinite{};

    REGISTER_YSON_STRUCT(TServiceLogControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceLogControllerState);

////////////////////////////////////////////////////////////////////////////////

class TServiceLogSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TServiceLogParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicServiceLogParameters);

    TServiceLogSourceController(
        TSourceControllerContextPtr context,
        TDynamicSourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;

    //! Build all the ranges and source keys from scratch. Deletes all the previous keys (thus partitions).
    void RebuildRanges(int partitionCount);

    //! Rebuilds ranges when needed and looks for missing ranges (happen after ranges traversal completes and partition dies).
    void EnsurePartitionsPresent();
    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() final;
    void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses) final;
    std::optional<TStreamTraverseDataPtr> GetFutureKeysStreamTraverseData() override;

protected:
    NLogging::TLogger Logger;

private:
    TMutableStateClient<TServiceLogControllerState> State_;
};

DEFINE_REFCOUNTED_TYPE(TServiceLogSourceController);

////////////////////////////////////////////////////////////////////////////////

class TServiceLogSource
    : public TOrderedSourceBase
{
    static constexpr int HashBlockSize_ = 1000000;
    static constexpr ui64 HashBlockCount_ = std::numeric_limits<ui64>::max() / HashBlockSize_;

public:
    using TSourceController = TServiceLogSourceController;
    using TOrderedSourceBase::UpdatePartitionInfo;
    using typename TOrderedSourceBase::TPartitionInfoUpdate;
    using typename TOrderedSourceBase::TRecord;
    using TPayloadRow = NTableClient::TUnversionedOwningRow;
    using TFetchResult = IServiceLogRowsProvider::TFetchResult;

    YT_FLOW_EXTEND_PARAMETERS(TServiceLogParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicServiceLogParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TDynamicServiceLogPartitionSpec);

public:
    TServiceLogSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

    std::vector<TRecord> ParseData(std::vector<TPayloadRow>&& rows, NTableClient::TTableSchemaPtr schema, TSystemTimestamp now);
    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimit) final;
    void DoReportPersistedOffset(TOffset offsetExclusive) final;

    void DoInit() final;
    void DoTerminate() final;

    bool IsFinite() final;

    i64 DoGetEstimatedRowsAtOffset(TOffset offset) final;

    bool AreOffsetsConsecutive() const final;
    bool CanCommittedOffsetExceedNextReadOffset() const final;
    TOffset GetNextOffset(const TOffset& offset) const final;
    std::string ConvertOffsetToLexicographicallyComparableString(const TOffset& offset) const final;

protected:
    const NLogging::TLogger Logger;

private:
    TServiceLogRangePtr Range_;
    IServiceLogRowsProviderPtr TableJoiner_;
    NConcurrency::IReconfigurableThroughputThrottlerPtr Throttler_;
    bool Finished_;
    TFuture<i64> ApproximateRowCountFuture_;
    TFuture<std::tuple<std::vector<TRecord>, bool, ui64>> ReadParsedCachedFuture_;
    //! Used to deal with empty reads that are bound by throttler.
    TOffset MinNextBatchOffset_;
    TOffset NextOffset_;

    static const TOffset FinishedOffset_;

    std::optional<TKey> TryGetOffsetKey(const TOffset& offset) const;
    std::optional<i64> TryGetOffsetShift(const TOffset& offset) const;

    TOffset CreateOffset(const TKey& key, i64 shift) const;

    NConcurrency::TThroughputThrottlerConfigPtr MakeThrottleConfig() const;
};

DEFINE_REFCOUNTED_TYPE(TServiceLogSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
