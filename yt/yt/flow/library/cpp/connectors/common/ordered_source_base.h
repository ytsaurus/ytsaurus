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

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <functional>
#include <list>
#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using TOffset = TKey;

i64 OffsetToInt(const TOffset& offset);
TOffset IntToOffset(i64 offset);

////////////////////////////////////////////////////////////////////////////////

//! Read settings as kept in the state. They compare by value, so equal settings match after a reload
//! re-encodes them; an entity stands for no settings.
struct TStateReadSettings
{
    NYTree::INodePtr Node;

    bool operator==(const TStateReadSettings& other) const;
};

void Serialize(const TStateReadSettings& settings, NYson::IYsonConsumer* consumer);
void Deserialize(TStateReadSettings& settings, NYTree::INodePtr node);
void Deserialize(TStateReadSettings& settings, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

struct TOrderedSourcePartitionState
    : public NYTree::TYsonStruct
{
    TOffset CommittedOffsetExclusive;
    TOffset PersistedOffsetExclusive;
    TOffset PublishedOffsetExclusive;
    TOffset MaxOffsetExclusive;

    bool MaxOffsetIsConfirmed{};

    TInstant LastNotIdleInstant;
    TInstant LastIdleInstant;

    TInstant LastUnavailableInstant;
    // Sum of the intervals between consecutively observed failures. Time when no instance was running
    // is not observed and therefore not counted.
    TDuration ObservedUnavailableDuration;

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

    TIntrusivePtr<TOrderedMemory<TUniqueSeqNo, TSystemTimestamp>> AlignmentTimestampMemory;
    TSystemTimestamp MaxMemorizedAlignmentTimestamp;

    //! Read settings of the reads in stable mode, keyed as #OffsetMemory. Null unless stable mode wrote it.
    TIntrusivePtr<TOrderedMemory<TOffset, TStateReadSettings>> ReadSettingsMemory;

    void EnsureInvariants() const;

    void NormalizeOffsets(const std::function<TOffset(const TOffset&)>& normalizeOffset);

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

        //! Column to materialize the source write time of the message into, in milliseconds.
        std::optional<std::string> WriteTimeColumn;

        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TExtendedDynamicParameters
        : public virtual TSourceBase::TDynamicParameters
    {
        TDuration UnavailableThreshold;

        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    TOrderedSourceBase(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

    //! Makes every #GetNextBatch return either nothing or exactly one whole read group; a group
    //! that is re-read follows the boundaries of its first read. Must be called before #Init, and only
    //! for a source class that declares `static constexpr bool ReplayStable = true`.
    void EnableStableReadGroups();

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

public:
    struct TRecord
    {
        TOffset Offset;
        TSystemTimestamp WriteTimestamp;
        TSystemTimestamp CreateTimestamp;

        std::optional<TFlowQueueMeta> Meta = {};

        std::vector<TPayload> Payloads;
        NTableClient::TTableSchemaPtr PayloadSchema;

        //! The write time for #TExtendedParameters::WriteTimeColumn when the source knows it more precisely than #WriteTimestamp.
        std::optional<ui64> WriteTimeMilliseconds = {};
    };

protected:
    struct TPartitionInfoUpdate
    {
        std::optional<TOffset> CommittedOffsetExclusive = {};
        std::optional<TOffset> MaxOffsetExclusive = {};
        std::optional<TInstant> UpdateInstant = {};
        //! The committed offset is an external position the connector chose to honor (a consumer
        //! offset), so reading past unread data is a deliberate skip, not evidence of trimming.
        bool Repositioned = false;
    };

    // To implement.

    virtual TOffset GetNextOffset(const TOffset& offset) const = 0;
    virtual TOffset NormalizeOffset(const TOffset& offset) const;
    virtual std::string ConvertOffsetToLexicographicallyComparableString(const TOffset& offset) const = 0;
    virtual bool AreOffsetsConsecutive() const = 0;
    virtual bool CanCommittedOffsetExceedNextReadOffset() const = 0;
    virtual i64 DoGetEstimatedRowsAtOffset(TOffset offset) = 0;

    virtual void DoInit()
    { }

    virtual void DoTerminate()
    { }

    virtual TFuture<std::vector<TRecord>> DoReadNextBatch(const TMessageBatcherSettingsPtr& settings, TOffset nextOffset, std::optional<TOffset> offsetLimitExclusive) = 0;

    //! Reads the records of [|nextOffset|, |endExclusive|), which were read before; batch limits of
    //! |settings| need not apply. May return a prefix, an empty one included, if no more records are
    //! available now.
    virtual TFuture<std::vector<TRecord>> DoReadRange(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        TOffset endExclusive);

    //! The offset before which the source has no records now, read from the broker; std::nullopt if the
    //! connector cannot tell. Called only when a replayed group shows an offset gap or makes no progress.
    virtual TFuture<std::optional<TOffset>> DoFetchTrimmedOffset();

    //! A snapshot of the connector's current dynamic settings that affect the content of records; a
    //! null value means there are none.
    virtual NYson::TYsonString CaptureReadSettings();

    //! Called right before every #DoReadNextBatch and #DoReadRange with settings once returned by
    //! #CaptureReadSettings, possibly by an earlier binary. Every record returned by the following call must be
    //! produced under |settings|: no other content-affecting dynamic setting is read, and no
    //! settings-dependent work is cached across calls. A missing key means the parameter's registered
    //! default, an unknown key is ignored, and an unknown value throws.
    virtual void ApplyReadSettings(const NYson::TYsonString& settings);

    // Report persisted offset to commit it to broker. Called repeatedly.
    // First time it called before DoInit().
    virtual void DoReportPersistedOffset(TOffset persistedOffsetExclusive) = 0;

    // By default it looks at GetParameters()->Finite, but it can be overriden for cases when partitions is limited by their nature.
    virtual bool IsFinite();

    virtual void AdjustInflight(const TInflightStreamTraverseDataPtr& inflight);

    //! Whether the payloads of the source already contain #TExtendedParameters::WriteTimeColumn.
    virtual bool MaterializesWriteTimeColumn() const;

    // To call from heirs.

    // Provide partition info from queue broker. Thread-safe.
    void UpdatePartitionInfo(const TPartitionInfoUpdate& update);

    //! Creates an error state that heirs update and that takes part in deciding whether the partition is
    //! available. Must be called from #DoInit().
    //! A failure of any such state is a failure; an OK from at least one, with none failing, is an OK.
    IStatusErrorStatePtr CreateAvailabilityErrorState(TStringBuf name);

    const NProfiling::TProfiler& GetProfiler() const;

    double GetSourceTotalCount() const;
    double GetSourceTotalBytes() const;
    std::optional<TSystemTimestamp> GetLastPersistedWriteTimestamp() const;
    TOffset GetKnownMaxOffsetExclusive() const;

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
    struct TReplayedReadGroup
    {
        TOffset EndExclusive;
        //! Read settings of the group's first read.
        NYson::TYsonString Settings;
        //! Records read so far; they are registered only once the group is complete.
        std::vector<TRecord> Records;
        i64 ReadCalls = 0;
        i64 PartialReturns = 0;
        TInstant OpenInstant;
    };

    TOffset NextReadOffset_;
    std::optional<TSystemTimestamp> ReadEventWatermark_;
    // May be greater than State_->PersistedOffsetExclusive_.
    // State_->PersistedOffsetExclusive_ may be delayed because it can not conflict with in flight messages.
    TOffset CommittedOffsetExclusive_;

    // Maximum alignment timestamp seen across all PrepareMessages calls.
    // Stored separately because TryCollapseOffsetInfo can merge InflightOffsets_ entries,
    // making InflightOffsets_.back().AlignmentTimestamp unreliable for the "last read" value.
    TSystemTimestamp LastReadAlignmentTimestamp_ = ZeroSystemTimestamp;

    //! The payload schema extended with the write time column, built for #WriteTimeSchemaBase_.
    NTableClient::TTableSchemaPtr WriteTimeSchema_;
    NTableClient::TTableSchemaPtr WriteTimeSchemaBase_;

    TMutableStateClient<TOrderedSourcePartitionState> State_;
    std::vector<IStatusErrorStatePtr> AvailabilityErrorStates_;

    TOffsetInfos InflightOffsets_;

    bool SourceMaxOffsetObserved_ = false;
    TSimpleEmaCounter SourceTotalCount_;
    TSimpleEmaCounter SourceTotalBytes_;
    TSimpleEmaCounter PersistedCount_;
    TSimpleEmaCounter PersistedBytes_;
    const NProfiling::TProfiler Profiler_;
    NProfiling::TCounter PersistedCountCounter_;
    NProfiling::TCounter PersistedBytesCounter_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, DelayedPartitionInfoUpdatesLock_);
    std::vector<TPartitionInfoUpdate> DelayedPartitionInfoUpdates_;

    bool StableReadGroups_ = false;
    std::optional<TReplayedReadGroup> ReplayedGroup_;
    NProfiling::TCounter ReplayPartialReturnsCounter_;

private:
    struct TReplayedGroupRead
    {
        TOffset NextOffset;
        TFuture<std::vector<TRecord>> Records;
    };

    void TryIncreaseMaxOffsetExclusive(TOffset newMaxOffsetExclusive, bool confirmed);
    std::vector<TMessageBatch> PrepareMessages(std::vector<TRecord>&& records);
    void AppendWriteTime(TRecord& record);

    TFuture<std::vector<TRecord>> ReadNextBatch(
        const TMessageBatcherSettingsPtr& batcherSettings,
        const std::optional<TOffset>& offsetLimitExclusive,
        const NYson::TYsonString& readSettings);
    void DropRecordsBeforeReadStart(std::vector<TRecord>* records, const TOffset& start);
    //! Like #DropRecordsBeforeReadStart, but also throws on a record past the open group's end.
    void ValidateReplayedGroupRecords(std::vector<TRecord>* records, const TOffset& start);
    //! Remembers the settings of a first-sight read whose last record is at |lastOffset|.
    void RegisterReadSettings(const TOffset& lastOffset, const NYson::TYsonString& settings);
    void OpenReadGroup();
    TOffset GetReadGroupNextOffset() const;
    //! Drops the buffered records a trim or a reposition has passed; returns whether this discarded
    //! the open group.
    bool DropStaleReadGroupRecords();
    //! Reads the open replayed group in one call: returns either nothing or the whole group.
    TFuture<std::vector<TMessageBatch>> ReadReplayedGroup(const TMessageBatcherSettingsPtr& settings);
    TReplayedGroupRead StartReplayedGroupRead(const TMessageBatcherSettingsPtr& settings);
    bool HasReadGroupGap(const std::vector<TRecord>& records) const;
    //! Applies a trim to |trimmedOffset| found by the connector; returns whether it discarded the open
    //! group.
    bool ApplyReadGroupTrim(const TOffset& trimmedOffset, std::vector<TRecord>* records);
    //! Returns std::nullopt if the replay loop goes on.
    std::optional<std::vector<TMessageBatch>> OnReplayedGroupRecords(
        std::vector<TRecord>&& records,
        bool progress,
        const TMessageBatcherSettingsPtr& settings,
        TCpuInstant loopStart);
    std::vector<TMessageBatch> ReturnIncompleteReadGroup(TStringBuf reason);
    std::vector<TMessageBatch> TakeReadGroup();
    void AdvanceOffsetMemory();
    void FlushDelayedPartitionInfoUpdates();
    void TryCollapseOffsetInfo(TOffsetInfos::iterator offsetInfoIt);
    void CleanUpInflightOffsets();
    //! |trimmed| is false for a deliberate skip, which is not worth the trimming error.
    void MarkMissingMessagesPersisted(bool trimmed = true);
    void UpdateUnavailability();
    void UpdateStatusProfilerMute();
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

inline constexpr TStringBuf MalformedMessagePolicyReadSetting = "malformed_message_policy";

//! Read settings that consist of `malformed_message_policy` |policy|.
template <class EPolicy>
NYson::TYsonString MakeMalformedMessagePolicyReadSettings(EPolicy policy);

//! The `malformed_message_policy` of |settings|, or |defaultPolicy|, the parameter's registered
//! default, if it is missing. Other keys are ignored, and an unknown value throws.
template <class EPolicy>
EPolicy ParseMalformedMessagePolicyReadSettings(const NYson::TYsonString& settings, EPolicy defaultPolicy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define ORDERED_SOURCE_BASE_INL_H_
#include "ordered_source_base-inl.h"
#undef ORDERED_SOURCE_BASE_INL_H_
