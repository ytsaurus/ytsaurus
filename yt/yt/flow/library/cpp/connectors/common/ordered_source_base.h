#pragma once

#include "public.h"

#include "flow_queue_meta.h"
#include "ordered_source.h"
#include "source_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/misc/counter.h>
#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <list>
#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using TOffset = TKey;

i64 OffsetToInt(const TOffset& offset);
TOffset IntToOffset(i64 offset);

////////////////////////////////////////////////////////////////////////////////

struct TOrderedSourcePartitionState
    : public NYTree::TYsonStruct
{
    TOffset CommittedOffsetExclusive;
    TOffset PersistedOffsetExclusive;
    TOffset PublishedOffsetExclusive;
    TOffset MaxOffsetExclusive;

    std::optional<i64> CommittedOffsetExclusiveObsolete;
    std::optional<i64> PersistedOffsetExclusiveObsolete;
    std::optional<i64> PublishedOffsetExclusiveObsolete;
    std::optional<i64> MaxOffsetExclusiveObsolete;

    bool MaxOffsetIsConfirmed{};

    TInstant LastNotIdleInstant;
    TInstant LastIdleInstant;

    TInstant LastUnavailableInstant;
    TDuration DecayedUnavailableDuration;

    // LastPersistedWriteTimestamp and FirstNotPersistedWriteTimestamp can be used for rough estimate of time lag.
    // LastPersistedWriteTimestamp is source write timestamp of last persisted message
    // and nullopt if all known messages in partition are committed.
    std::optional<TSystemTimestamp> LastPersistedWriteTimestamp;
    std::optional<TSystemTimestamp> FirstNotPersistedWriteTimestamp;

    // Extracted from flow queue meta heartbeats.
    std::optional<TSystemTimestamp> PersistedEventWatermark;

    TMessageId PersistedMessageIdExclusive;

    double AvgOffsetCountSize{};
    double AvgOffsetByteSize{};

    TIntrusivePtr<TOrderedMemory<TOffset, TUniqueSeqNo>> OffsetMemory;
    TIntrusivePtr<TOrderedMemory<i64, TUniqueSeqNo>> OffsetMemoryObsolete;

    TIntrusivePtr<TOrderedMemory<TUniqueSeqNo, TSystemTimestamp>> AlignmentTimestampMemory;
    TSystemTimestamp MaxMemorizedAlignmentTimestamp;

    void EnsureInvariants() const;

    void SyncObsoleteOffsets();

    REGISTER_YSON_STRUCT(TOrderedSourcePartitionState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOrderedSourcePartitionState);

////////////////////////////////////////////////////////////////////////////////

class TOrderedSourceBase
    : public IOrderedSource
    , public TSourceBase
{
private:
    struct TExtendedParameters
        : public virtual TSourceBase::TParameters
    {
        // For finite pipelines.
        bool Finite{};

        TDuration UpdateInfoPeriod;

        // TODO(mikari): better name
        double ByteSizeAlpha{};

        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TExtendedDynamicParameters
        : public virtual TSourceBase::TDynamicParameters
    {
        TDuration UnavailableTimeHalfDecayPeriod;

        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    TOrderedSourceBase(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Terminate() final;
    void Sync() final;
    void Commit() final;

    void MarkPublished(const TSourceMessageBatchCookie& cookie) final;
    void MarkPersisted(const TSourceMessageBatchCookie& cookie) final;
    TMessageId GetMaxPersistedMessageIdExclusive() final;
    TSystemTimestamp GetReadAlignmentTimestamp() final;

    TFuture<std::vector<TMessageBatch>> GetNextBatch(const TMessageBatcherSettingsPtr& settings) final;

    TInflightStreamTraverseDataPtr BuildInflight() final;

    std::optional<TSystemTimestamp> GetPersistedEventWatermark() final;
    std::optional<TSystemTimestamp> GetReadEventWatermark() final;
    TDuration GetAlignmentTimestampWindow() final;

protected:
    const NLogging::TLogger Logger;

protected:
    struct TRecord
    {
        TOffset Offset;
        TSystemTimestamp WriteTimestamp;
        TSystemTimestamp CreateTimestamp;

        std::optional<TFlowQueueMeta> Meta = {};

        std::vector<TPayload> Payloads;
        NTableClient::TTableSchemaPtr PayloadSchema;
    };

    struct TPartitionInfoUpdate
    {
        std::optional<TOffset> CommittedOffsetExclusive = {};
        std::optional<TOffset> MaxOffsetExclusive = {};
        std::optional<TInstant> UpdateInstant = {};
    };

    // To implement.

    virtual TOffset GetNextOffset(const TOffset& offset) const = 0;
    virtual std::string ConvertOffsetToLexicographicallyComparableString(const TOffset& offset) const = 0;
    virtual bool AreOffsetsConsecutive() const = 0;
    virtual bool CanCommittedOffsetExceedNextReadOffset() const = 0;
    virtual i64 DoGetEstimatedRowsAtOffset(TOffset offset) = 0;

    virtual void DoInit()
    { }

    virtual void DoTerminate()
    { }

    virtual TFuture<std::vector<TRecord>> DoReadNextBatch(const TMessageBatcherSettingsPtr& settings, TOffset nextOffset, std::optional<TOffset> offsetLimitExclusive) = 0;

    // Report persisted offset to commit it to broker. Called repeatedly.
    // First time it called before DoInit().
    virtual void DoReportPersistedOffset(TOffset persistedOffsetExclusive) = 0;

    // By default it looks at GetParameters()->Finite, but it can be overriden for cases when partitions is limited by their nature.
    virtual bool IsFinite();

    // To call from heirs.

    // Provide partition info from queue broker. Thread-safe.
    void UpdatePartitionInfo(const TPartitionInfoUpdate& update);

    // This error state can be updated from heirs and used for estimating availability of source partition.
    IStatusErrorStatePtr GetReadErrorState() const;

    const NProfiling::TProfiler& GetProfiler() const;

public:
    enum class EMessageState
    {
        New,
        Published,
        Persisted,
    };

    struct TOffsetInfo
    {
        TOffset Offset;

        EMessageState State = EMessageState::New;

        // Next fields can be aggregated if some offset is processed before previous one.

        // Used to generate PersistedMessageIdExclusive on demand.
        TUniqueSeqNo NextMessageSeqNo;
        TOffset NextOffset;

        TSystemTimestamp WriteTimestamp;
        TSystemTimestamp AlignmentTimestamp;
        std::optional<TSystemTimestamp> EventWatermark;
        i64 Count{};
        i64 ByteSize{};
    };

    // Stable iterators are important.
    using TOffsetInfos = std::list<TOffsetInfo>;

private:
    TOffset NextReadOffset_;
    std::optional<TSystemTimestamp> ReadEventWatermark_;
    // May be greater than State_->PersistedOffsetExclusive_.
    // State_->PersistedOffsetExclusive_ may be delayed because it can not conflict with in flight messages.
    TOffset CommittedOffsetExclusive_;

    // Maximum alignment timestamp seen across all PrepareMessages calls.
    // Stored separately because TryCollapseOffsetInfo can merge InflightOffsets_ entries,
    // making InflightOffsets_.back().AlignmentTimestamp unreliable for the "last read" value.
    TSystemTimestamp LastReadAlignmentTimestamp_ = ZeroSystemTimestamp;

    TInstant InitInstant_;

    TMutableStateClient<TOrderedSourcePartitionState> State_;
    IStatusErrorStatePtr ReadErrorState_;

    TOffsetInfos InflightOffsets_;

    TSimpleEmaCounter SourceTotalCount_;
    TSimpleEmaCounter SourceTotalBytes_;
    TSimpleEmaCounter PersistedCount_;
    TSimpleEmaCounter PersistedBytes_;
    const NProfiling::TProfiler Profiler_;
    NProfiling::TCounter PersistedCountCounter_;
    NProfiling::TCounter PersistedBytesCounter_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DelayedPartitionInfoUpdatesLock_);
    std::vector<TPartitionInfoUpdate> DelayedPartitionInfoUpdates_;

private:
    void TryIncreaseMaxOffsetExclusive(TOffset newMaxOffsetExclusive, bool confirmed);
    std::vector<TMessageBatch> PrepareMessages(std::vector<TRecord>&& records);
    void FlushDelayedPartitionInfoUpdates();
    void TryCollapseOffsetInfo(TOffsetInfos::iterator offsetInfoIt);
    void CleanUpInflightOffsets();
    void MarkMissingMessagesPersisted();
    void UpdateUnavailability();
};

////////////////////////////////////////////////////////////////////////////////

class TIntegerOffsetOrderedSourceBase
    : public TOrderedSourceBase
{
public:
    using TOrderedSourceBase::TOrderedSourceBase;

protected:
    TOffset GetNextOffset(const TOffset& offset) const final;
    std::string ConvertOffsetToLexicographicallyComparableString(const TOffset& offset) const final;
    bool AreOffsetsConsecutive() const final;
    bool CanCommittedOffsetExceedNextReadOffset() const final;
    i64 DoGetEstimatedRowsAtOffset(TOffset offset) final;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
