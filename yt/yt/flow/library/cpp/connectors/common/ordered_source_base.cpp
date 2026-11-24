#include "ordered_source_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void TryIncrease(T& a, const T& b)
{
    if (a < b) {
        a = b;
    }
}

////////////////////////////////////////////////////////////////////////////////

// Object stored inside TSourceMessageBatchCookie (via std::any).
// Holds an iterator to the offset info entry, allowing O(1) MarkPublished/MarkPersisted.
struct TMessageCookieData
{
    TOrderedSourceBase::TOffsetInfos::iterator OffsetInfoIt;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRecords>
i64 EraseRecordsBefore(TRecords* records, const TOffset& offset)
{
    return std::erase_if(*records, [&] (const auto& record) {
        return record.Offset < offset;
    });
}

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr ReadSettingsToNode(const NYson::TYsonString& settings)
{
    return settings ? NYTree::ConvertToNode(settings) : NYTree::GetEphemeralNodeFactory()->CreateEntity();
}

NYson::TYsonString ReadSettingsFromNode(const NYTree::INodePtr& node)
{
    return node->GetType() == NYTree::ENodeType::Entity ? NYson::TYsonString() : NYson::ConvertToYsonString(node);
}

std::string FormatReadSettings(const NYson::TYsonString& settings)
{
    if (!settings) {
        return "<none>";
    }

    return std::string(NYson::ConvertToYsonString(settings, NYson::EYsonFormat::Text).AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TStateReadSettings::operator==(const TStateReadSettings& other) const
{
    return NYTree::AreNodesEqual(Node, other.Node);
}

void Serialize(const TStateReadSettings& settings, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(settings.Node, consumer);
}

void Deserialize(TStateReadSettings& settings, NYTree::INodePtr node)
{
    settings.Node = std::move(node);
}

void Deserialize(TStateReadSettings& settings, NYson::TYsonPullParserCursor* cursor)
{
    NYTree::Deserialize(settings.Node, cursor);
}

////////////////////////////////////////////////////////////////////////////////

i64 OffsetToInt(const TOffset& offset)
{
    if (offset.Underlying().GetCount() == 0) {
        return 0;
    }
    YT_VERIFY(offset.Underlying().GetCount() == 1);
    return NTableClient::FromUnversionedValue<i64>(offset.Underlying()[0]);
}

TOffset IntToOffset(i64 offset)
{
    if (offset == 0) {
        // We want default-constructed offset be the same as offset = 0.
        return MakeKey();
    }

    return MakeKey(offset);
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedSourcePartitionState::EnsureInvariants() const
{
    THROW_ERROR_EXCEPTION_UNLESS(CommittedOffsetExclusive <= PersistedOffsetExclusive,
        "Committed offset %v is greater than persisted offset %v",
        CommittedOffsetExclusive,
        PersistedOffsetExclusive);
    THROW_ERROR_EXCEPTION_UNLESS(PersistedOffsetExclusive <= PublishedOffsetExclusive,
        "Persisted offset %v is greater than published offset %v",
        PersistedOffsetExclusive,
        PublishedOffsetExclusive);
    THROW_ERROR_EXCEPTION_UNLESS(PublishedOffsetExclusive <= MaxOffsetExclusive,
        "Published offset %v is greater than max offset %v",
        PublishedOffsetExclusive,
        MaxOffsetExclusive);
    THROW_ERROR_EXCEPTION_UNLESS(LastIdleInstant >= LastNotIdleInstant || LastIdleInstant == TInstant::Zero(),
        "Last idle instant %v is unexpectedly less than last not-idle instant %v",
        LastIdleInstant,
        LastNotIdleInstant);
}

void TOrderedSourcePartitionState::NormalizeOffsets(
    const std::function<TOffset(const TOffset&)>& normalizeOffset)
{
    CommittedOffsetExclusive = normalizeOffset(CommittedOffsetExclusive);
    PersistedOffsetExclusive = normalizeOffset(PersistedOffsetExclusive);
    PublishedOffsetExclusive = normalizeOffset(PublishedOffsetExclusive);
    MaxOffsetExclusive = normalizeOffset(MaxOffsetExclusive);

    // OffsetMemory keys remain version-specific. Switching formats with nonempty memory is readable,
    // but a completed drain is required to preserve replay sequence numbers and exactly-once semantics.
    EnsureInvariants();
}

void TOrderedSourcePartitionState::Register(TRegistrar registrar)
{
    registrar.Parameter("committed_offset_exclusive_v2", &TThis::CommittedOffsetExclusive)
        .Default(MakeKey());
    registrar.Parameter("persisted_offset_exclusive_v2", &TThis::PersistedOffsetExclusive)
        .Default(MakeKey());
    registrar.Parameter("published_offset_exclusive_v2", &TThis::PublishedOffsetExclusive)
        .Default(MakeKey());
    registrar.Parameter("max_offset_exclusive_v2", &TThis::MaxOffsetExclusive)
        .Default(MakeKey());

    registrar.Parameter("max_offset_is_confirmed", &TThis::MaxOffsetIsConfirmed)
        .Default(false);

    registrar.Parameter("last_not_idle_instant", &TThis::LastNotIdleInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("last_idle_instant", &TThis::LastIdleInstant)
        .Default(TInstant::Zero());

    registrar.Parameter("last_unavailable_instant", &TThis::LastUnavailableInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("observed_unavailable_duration", &TThis::ObservedUnavailableDuration)
        .Default(TDuration::Zero());

    registrar.Parameter("last_persisted_write_timestamp", &TThis::LastPersistedWriteTimestamp)
        .Default();
    registrar.Parameter("first_not_persisted_write_timestamp", &TThis::FirstNotPersistedWriteTimestamp)
        .Default();

    registrar.Parameter("persisted_event_watermark", &TThis::PersistedEventWatermark)
        .Default();

    registrar.Parameter("persisted_message_id_exclusive", &TThis::PersistedMessageIdExclusive)
        .Default();

    registrar.Parameter("avg_offset_count_size", &TThis::AvgOffsetCountSize)
        .Default(1.0);
    registrar.Parameter("avg_offset_byte_size", &TThis::AvgOffsetByteSize)
        .Default(1.0);

    registrar.Parameter("offset_memory_v2", &TThis::OffsetMemory)
        .DefaultNew();

    registrar.Parameter("alignment_timestamp_memory", &TThis::AlignmentTimestampMemory)
        .DefaultNew();
    registrar.Parameter("max_memorized_alignment_timestamp", &TThis::MaxMemorizedAlignmentTimestamp)
        .Default(ZeroSystemTimestamp);

    registrar.Parameter("read_settings_memory", &TThis::ReadSettingsMemory)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedSourceBase::TExtendedParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("finite", &TThis::Finite)
        .Default(false);
    registrar.Parameter("update_info_period", &TThis::UpdateInfoPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("byte_size_alpha", &TThis::ByteSizeAlpha)
        .Default(0.05);
    registrar.Parameter("write_time_column", &TThis::WriteTimeColumn)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedSourceBase::TExtendedDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("unavailable_threshold", &TThis::UnavailableThreshold)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedSourceBase::TryIncreaseMaxOffsetExclusive(TOffset newMaxOffsetExclusive, bool confirmed)
{
    if (newMaxOffsetExclusive > State_->MaxOffsetExclusive) {
        const TOffset oldMaxOffsetExclusive = State_->MaxOffsetExclusive;
        State_->MaxOffsetExclusive = newMaxOffsetExclusive;
        State_->MaxOffsetIsConfirmed = confirmed;
        i64 deltaRows = DoGetEstimatedRowsAtOffset(newMaxOffsetExclusive) - DoGetEstimatedRowsAtOffset(oldMaxOffsetExclusive);
        SourceTotalCount_.Inc(deltaRows * State_->AvgOffsetCountSize);
        SourceTotalBytes_.Inc(deltaRows * State_->AvgOffsetByteSize);
    } else if (newMaxOffsetExclusive == State_->MaxOffsetExclusive) {
        if (confirmed) {
            SourceTotalCount_.Inc(0);
            SourceTotalBytes_.Inc(0);
        }
        State_->MaxOffsetIsConfirmed |= confirmed;
    }

    if (confirmed && !SourceMaxOffsetObserved_ && newMaxOffsetExclusive == State_->MaxOffsetExclusive) {
        // The first observed maximum is inventory, not an arrival interval.
        auto now = TInstant::Now();
        SourceTotalCount_.ResetRate(now);
        SourceTotalBytes_.ResetRate(now);
        SourceMaxOffsetObserved_ = true;
    }
}

TOrderedSourceBase::TOrderedSourceBase(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TSourceBase(std::move(context), std::move(dynamicContext))
    , Logger(TSourceBase::Logger)
    , NextReadOffset_(MakeKey())
    , CommittedOffsetExclusive_(MakeKey())
    , Profiler_(GetContext()->Profiler)
    , PersistedCountCounter_(Profiler_.Counter("/persisted_count"))
    , PersistedBytesCounter_(Profiler_.Counter("/persisted_bytes"))
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(GetContext()->SerializedInvoker->IsSerialized());
}

void TOrderedSourceBase::Init(IInitContextPtr initContext)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    initContext->InitClient<TOrderedSourcePartitionState>(State_, "v0");

    State_->NormalizeOffsets([this] (const TOffset& offset) {
        return NormalizeOffset(offset);
    });

    NextReadOffset_ = State_->PersistedOffsetExclusive;
    ReadEventWatermark_ = State_->PersistedEventWatermark;
    CommittedOffsetExclusive_ = State_->CommittedOffsetExclusive;
    DoReportPersistedOffset(State_->PersistedOffsetExclusive);
    DoInit();

    SourceTotalCount_.Update(0);
    SourceTotalBytes_.Update(0);
    PersistedCount_.Update(0);
    PersistedBytes_.Update(0);

    UpdateStatusProfilerMute();
    SubscribeReconfigured(
        BIND([this] (const TDynamicSourceContextPtr& /*dynamicContext*/) {
            UpdateStatusProfilerMute();
        }));
}

void TOrderedSourceBase::Terminate()
{
    // This method can be called from ControlInvoker.
    BIND([this, this_ = MakeStrong(this)] () {
        DoTerminate();
    })
        .Via(GetContext()->SerializedInvoker)
        .Run();
}

void TOrderedSourceBase::Sync()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);
    CleanUpInflightOffsets();
    FlushDelayedPartitionInfoUpdates();

    if (State_->OffsetMemory->empty()) {
        State_->AlignmentTimestampMemory->clear();
    } else {
        State_->AlignmentTimestampMemory->AdvanceExclusive(State_->OffsetMemory->front().second);
    }

    State_->EnsureInvariants();

    // Debug offset memory state corrupting.
    if (!State_->OffsetMemory->empty()) {
        TOffset lastOffset = State_->PersistedOffsetExclusive;
        static const ui64 minPossibleSeqNo = NTransactionClient::TimestampFromUnixTime(TInstant::ParseIso8601("2025-01-01").Seconds()).Underlying();
        ui64 lastSeqNo = minPossibleSeqNo;
        for (auto& [offset, seqNo] : *State_->OffsetMemory) {
            YT_VERIFY(offset >= lastOffset);
            YT_VERIFY(seqNo.Underlying() >= lastSeqNo);
            lastOffset = offset;
            lastSeqNo = seqNo.Underlying();
        }
        YT_VERIFY(lastOffset < State_->MaxOffsetExclusive);
    }
}

void TOrderedSourceBase::Commit()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    YT_TLOG_INFO("Persisted state")
        .With("State", NYson::ConvertToYsonString(State_, NYson::EYsonFormat::Text))
        .With("NextOffset", NextReadOffset_);

    DoReportPersistedOffset(State_->PersistedOffsetExclusive);
}

void TOrderedSourceBase::UpdatePartitionInfo(
    const TPartitionInfoUpdate& update)
{
    const TInstant now = update.UpdateInstant.has_value() ? *update.UpdateInstant : TInstant::Now();
    auto guard = Guard(DelayedPartitionInfoUpdatesLock_);
    DelayedPartitionInfoUpdates_.push_back(update);
    DelayedPartitionInfoUpdates_.back().UpdateInstant = now;
}

IStatusErrorStatePtr TOrderedSourceBase::CreateAvailabilityErrorState(TStringBuf name)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    auto errorState = GetContext()->StatusProfiler->ErrorState(name);
    AvailabilityErrorStates_.push_back(errorState);
    return errorState;
}

const NProfiling::TProfiler& TOrderedSourceBase::GetProfiler() const
{
    return Profiler_;
}

double TOrderedSourceBase::GetSourceTotalCount() const
{
    return SourceTotalCount_.GetTotal();
}

double TOrderedSourceBase::GetSourceTotalBytes() const
{
    return SourceTotalBytes_.GetTotal();
}

std::optional<TSystemTimestamp> TOrderedSourceBase::GetLastPersistedWriteTimestamp() const
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);
    return State_->LastPersistedWriteTimestamp;
}

TOffset TOrderedSourceBase::GetKnownMaxOffsetExclusive() const
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);
    return State_->MaxOffsetExclusive;
}

TOffset TOrderedSourceBase::NormalizeOffset(const TOffset& offset) const
{
    return offset;
}

void TOrderedSourceBase::FlushDelayedPartitionInfoUpdates()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    std::vector<TPartitionInfoUpdate> updates;
    {
        auto guard = Guard(DelayedPartitionInfoUpdatesLock_);
        updates = std::move(DelayedPartitionInfoUpdates_);
        DelayedPartitionInfoUpdates_.clear();
    }

    for (const auto& update : updates) {
        try {
            YT_VERIFY(update.UpdateInstant.has_value());

            YT_TLOG_DEBUG("Apply partition info update")
                .With("CommittedOffsetExclusive", update.CommittedOffsetExclusive)
                .With("MaxOffsetExclusive", update.MaxOffsetExclusive);

            if (update.CommittedOffsetExclusive) {
                CommittedOffsetExclusive_ = std::max(CommittedOffsetExclusive_, *update.CommittedOffsetExclusive);
                TryIncreaseMaxOffsetExclusive(*update.CommittedOffsetExclusive, false);
            }
            if (CommittedOffsetExclusive_ > NextReadOffset_) {
                if (update.Repositioned) {
                    YT_TLOG_INFO("Skip NextReadOffset forward to an external position")
                        .With("NewNextReadOffset", CommittedOffsetExclusive_)
                        .With("OldNextReadOffset", NextReadOffset_);
                } else if (!CanCommittedOffsetExceedNextReadOffset()) {
                    YT_TLOG_ERROR("Rewind NextReadOffset up to CommittedOffsetExclusive. Probably some input data was trimmed before reading")
                        .With("NewNextReadOffset", CommittedOffsetExclusive_)
                        .With("OldNextReadOffset", NextReadOffset_);
                }
                NextReadOffset_ = CommittedOffsetExclusive_;
                MarkMissingMessagesPersisted(/*trimmed*/ !update.Repositioned);
            }
            if (update.MaxOffsetExclusive) {
                TryIncreaseMaxOffsetExclusive(*update.MaxOffsetExclusive, true);
                if (State_->MaxOffsetExclusive > State_->CommittedOffsetExclusive || !State_->MaxOffsetIsConfirmed) {
                    State_->LastNotIdleInstant = *update.UpdateInstant;
                    State_->LastIdleInstant = TInstant::Zero();
                } else if (*update.UpdateInstant > State_->LastNotIdleInstant) {
                    State_->LastIdleInstant = std::max(State_->LastIdleInstant, *update.UpdateInstant);
                    if (State_->LastNotIdleInstant == TInstant::Zero()) { // If it is the first update of MaxOffsetExclusive.
                        State_->LastNotIdleInstant = State_->LastIdleInstant;
                    }
                    State_->LastPersistedWriteTimestamp = {};
                }
            }
            State_->EnsureInvariants();
        } catch (const std::exception& ex) {
            YT_TLOG_FATAL("Fatal error in updating partition info")
                .With(ex);
        }
    }
    if (CommittedOffsetExclusive_ > State_->CommittedOffsetExclusive) {
        State_->CommittedOffsetExclusive = std::min(CommittedOffsetExclusive_, State_->PersistedOffsetExclusive);
    }
}

void TOrderedSourceBase::MarkPublished(const TSourceMessageBatchCookie& cookie)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    const auto& cookieData = std::any_cast<const TMessageCookieData&>(cookie.Underlying());
    YT_VERIFY(cookieData.OffsetInfoIt->State == EMessageState::New);
    cookieData.OffsetInfoIt->State = EMessageState::Published;
    TryIncrease(State_->PublishedOffsetExclusive, cookieData.OffsetInfoIt->NextOffset);
}

void TOrderedSourceBase::MarkPersisted(const TSourceMessageBatchCookie& cookie)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    const auto& cookieData = std::any_cast<const TMessageCookieData&>(cookie.Underlying());
    YT_VERIFY(cookieData.OffsetInfoIt->State != EMessageState::Persisted);
    cookieData.OffsetInfoIt->State = EMessageState::Persisted;

    TryCollapseOffsetInfo(cookieData.OffsetInfoIt);
}

void TOrderedSourceBase::TryCollapseOffsetInfo(TOffsetInfos::iterator offsetInfoIt)
{
    YT_ASSERT(offsetInfoIt != InflightOffsets_.end());
    if (offsetInfoIt == InflightOffsets_.begin() || offsetInfoIt->State != EMessageState::Persisted) {
        return;
    }

    auto prevIt = std::prev(offsetInfoIt);

    prevIt->NextMessageSeqNo = offsetInfoIt->NextMessageSeqNo;
    prevIt->NextOffset = offsetInfoIt->NextOffset;
    TryIncrease(prevIt->WriteTimestamp, offsetInfoIt->WriteTimestamp);
    TryIncrease(prevIt->EventWatermark, offsetInfoIt->EventWatermark);
    prevIt->Count += offsetInfoIt->Count;
    prevIt->ByteSize += offsetInfoIt->ByteSize;

    InflightOffsets_.erase(offsetInfoIt);
}

void TOrderedSourceBase::CleanUpInflightOffsets()
{
    i64 persistedCount = 0;
    i64 persistedBytes = 0;

    while (!InflightOffsets_.empty()) {
        auto& offsetInfo = *InflightOffsets_.begin();
        if (offsetInfo.State != EMessageState::Persisted) {
            break;
        }
        // Generate the persisted-bound message id lazily — the seqNo + offset pair was kept
        // on the entry instead of a pre-allocated TMessageId string. See TOffsetInfo comment.
        auto persistedMessageIdExclusive = GenerateOrderedMessageId(
            offsetInfo.NextMessageSeqNo,
            GetContext()->SourceStreamId,
            ConvertOffsetToLexicographicallyComparableString(offsetInfo.NextOffset));
        YT_TLOG_FATAL_UNLESS(persistedMessageIdExclusive >= State_->PersistedMessageIdExclusive,
            "Expected that new persisted message id is greater than persisted in state")
            .With("NewPersistedMessageIdExclusive", persistedMessageIdExclusive)
            .With("StatePersistedMessageIdExclusive", State_->PersistedMessageIdExclusive);
        State_->PersistedMessageIdExclusive = std::move(persistedMessageIdExclusive);
        TryIncrease(State_->PersistedOffsetExclusive, offsetInfo.NextOffset);
        TryIncrease(State_->PublishedOffsetExclusive, offsetInfo.NextOffset);
        TryIncrease(State_->LastPersistedWriteTimestamp, std::optional(offsetInfo.WriteTimestamp));
        TryIncrease(State_->PersistedEventWatermark, offsetInfo.EventWatermark);
        persistedCount += offsetInfo.Count;
        persistedBytes += offsetInfo.ByteSize;

        InflightOffsets_.pop_front();
    }

    PersistedCount_.Inc(persistedCount);
    PersistedCountCounter_.Increment(persistedCount);
    PersistedBytes_.Inc(persistedBytes);
    PersistedBytesCounter_.Increment(persistedBytes);

    AdvanceOffsetMemory();
    MarkMissingMessagesPersisted();
}

void TOrderedSourceBase::AdvanceOffsetMemory()
{
    State_->OffsetMemory->AdvanceExclusive(State_->PersistedOffsetExclusive);

    if (State_->ReadSettingsMemory) {
        State_->ReadSettingsMemory->AdvanceExclusive(State_->PersistedOffsetExclusive);
    }
}

void TOrderedSourceBase::MarkMissingMessagesPersisted(bool trimmed)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    const auto& firstNotPersistedMessageOffset = InflightOffsets_.empty() ? NextReadOffset_ : InflightOffsets_.front().Offset;

    YT_TLOG_FATAL_UNLESS(firstNotPersistedMessageOffset >= State_->PersistedOffsetExclusive,
        "Expected increasing of offsets")
        .With("NewPersistedOffsetExclusive", firstNotPersistedMessageOffset)
        .With("OldPersistedOffsetExclusive", State_->PersistedOffsetExclusive);
    if (firstNotPersistedMessageOffset > State_->PersistedOffsetExclusive) {
        if (trimmed && AreOffsetsConsecutive()) {
            YT_TLOG_ERROR("Rewind PersistedOffsetExclusive up to first not persisted message offset. Probably some input data was trimmed before reading")
                .With("OldPersistedOffsetExclusive", State_->PersistedOffsetExclusive)
                .With("FirstNotPersistedMessageOffset", firstNotPersistedMessageOffset);
        }
        State_->PersistedOffsetExclusive = firstNotPersistedMessageOffset;
        State_->PublishedOffsetExclusive = std::max(State_->PublishedOffsetExclusive, State_->PersistedOffsetExclusive);
        State_->EnsureInvariants();
        AdvanceOffsetMemory();

        TUniqueSeqNo seqNoWatermark = {};
        if (State_->OffsetMemory->empty()) {
            seqNoWatermark = NConcurrency::WaitFor(GetContext()->TimeProvider->GenerateGlobalUniqueSeqNo()).ValueOrThrow().UniqueSeqNo;
        } else {
            seqNoWatermark = State_->OffsetMemory->front().second;
        }

        const TMessageId newPersistedMessageIdExclusive = GenerateOrderedMessageId(
            seqNoWatermark,
            GetContext()->SourceStreamId,
            ConvertOffsetToLexicographicallyComparableString(firstNotPersistedMessageOffset));
        YT_TLOG_FATAL_UNLESS(newPersistedMessageIdExclusive >= State_->PersistedMessageIdExclusive,
            "Expected increasing of message ids")
            .With("NewPersistedMessageId", newPersistedMessageIdExclusive)
            .With("OldPersistedMessageId", State_->PersistedMessageIdExclusive);
        State_->PersistedMessageIdExclusive = newPersistedMessageIdExclusive;
    }

    if (!InflightOffsets_.empty()) {
        State_->FirstNotPersistedWriteTimestamp = InflightOffsets_.front().WriteTimestamp;
    } else {
        State_->FirstNotPersistedWriteTimestamp = {};
    }
}

TMessageId TOrderedSourceBase::GetMaxPersistedMessageIdExclusive()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    return State_->PersistedMessageIdExclusive;
}

TFuture<std::vector<ISource::TMessageBatch>> TOrderedSourceBase::GetNextBatch(
    const TMessageBatcherSettingsPtr& batcherSettings)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    FlushDelayedPartitionInfoUpdates();

    if (StableReadGroups_) {
        DropStaleReadGroupRecords();
    }

    std::optional<TOffset> offsetLimitExclusive;
    if (IsDraining()) {
        const bool publishedOffsetReached = NextReadOffset_ >= State_->PublishedOffsetExclusive;
        if (publishedOffsetReached) {
            return MakeFuture<std::vector<ISource::TMessageBatch>>({});
        } else {
            offsetLimitExclusive = State_->PublishedOffsetExclusive;
        }
    }

    YT_VERIFY(NextReadOffset_ >= State_->PersistedOffsetExclusive);

    if (StableReadGroups_) {
        OpenReadGroup();

        if (ReplayedGroup_) {
            // A group with messages ends at or before the published offset, which bounds a drain.
            if (offsetLimitExclusive && *offsetLimitExclusive < ReplayedGroup_->EndExclusive) {
                return MakeFuture<std::vector<ISource::TMessageBatch>>(TError("Drain limit %v is inside replayed read group ending at %v",
                    *offsetLimitExclusive,
                    ReplayedGroup_->EndExclusive));
            }

            return ReadReplayedGroup(batcherSettings);
        }
    }

    const auto start = NextReadOffset_;
    auto settings = CaptureReadSettings();
    return ReadNextBatch(batcherSettings, offsetLimitExclusive, settings)
        .AsUnique()
        .Apply(BIND([weakThis = MakeWeak(this), start, settings = std::move(settings)] (TErrorOr<std::vector<TRecord>>&& result) {
            if (auto strongThis = weakThis.Lock()) {
                auto records = std::move(result.ValueOrThrow());
                if (!strongThis->StableReadGroups_) {
                    return strongThis->PrepareMessages(std::move(records));
                }

                // A first-sight read is a whole group.
                strongThis->DropRecordsBeforeReadStart(&records, start);
                std::optional<TOffset> lastOffset;
                if (!records.empty()) {
                    lastOffset = records.back().Offset;
                }

                auto batches = strongThis->PrepareMessages(std::move(records));
                if (lastOffset) {
                    strongThis->RegisterReadSettings(*lastOffset, settings);
                }

                return batches;
            }

            THROW_ERROR_EXCEPTION("Interrupted");
        })
                .AsyncVia(GetCurrentInvoker()));
}

bool TOrderedSourceBase::MaterializesWriteTimeColumn() const
{
    return false;
}

void TOrderedSourceBase::AppendWriteTime(TRecord& record)
{
    const auto& schema = record.PayloadSchema;
    if (WriteTimeSchemaBase_ != schema) {
        const auto& column = *GetParameters()->WriteTimeColumn;
        THROW_ERROR_EXCEPTION_IF(schema->FindColumn(column),
            "Write time column %Qv is already present in the source schema",
            column);
        auto columns = schema->Columns();
        columns.emplace_back(column, NTableClient::EValueType::Uint64);
        WriteTimeSchema_ = New<NTableClient::TTableSchema>(std::move(columns), schema->IsStrict(), schema->IsUniqueKeys());
        WriteTimeSchemaBase_ = schema;
    }
    const ui64 writeTime = record.WriteTimeMilliseconds.value_or(record.WriteTimestamp.Underlying() * 1000);
    const int writeTimeId = schema->GetColumnCount();
    for (auto& payload : record.Payloads) {
        const auto row = payload.Underlying().Get();
        THROW_ERROR_EXCEPTION_UNLESS(static_cast<int>(row.GetCount()) == writeTimeId,
            "Source row does not match its schema")
            .With("value_count", row.GetCount())
            .With("column_count", writeTimeId);
        size_t stringDataSize = 0;
        for (const auto& value : row) {
            if (NTableClient::IsStringLikeType(value.Type)) {
                stringDataSize += value.Length;
            }
        }
        payload = TPayload(TPayload::TUnderlying(writeTimeId + 1, stringDataSize, [&] (NTableClient::TMutableUnversionedRow mutableRow) {
            std::copy(row.begin(), row.end(), mutableRow.begin());
            mutableRow[writeTimeId] = NTableClient::MakeUnversionedUint64Value(writeTime, writeTimeId);
        }));
    }
    record.PayloadSchema = WriteTimeSchema_;
}

TFuture<std::vector<TOrderedSourceBase::TRecord>> TOrderedSourceBase::ReadNextBatch(
    const TMessageBatcherSettingsPtr& batcherSettings,
    const std::optional<TOffset>& offsetLimitExclusive,
    const NYson::TYsonString& readSettings)
{
    try {
        ApplyReadSettings(readSettings);
        return DoReadNextBatch(batcherSettings, NextReadOffset_, offsetLimitExclusive);
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TRecord>>(TError(ex));
    }
}

TFuture<std::vector<TOrderedSourceBase::TRecord>> TOrderedSourceBase::DoReadRange(
    const TMessageBatcherSettingsPtr& settings,
    TOffset nextOffset,
    TOffset endExclusive)
{
    return DoReadNextBatch(settings, std::move(nextOffset), std::move(endExclusive));
}

void TOrderedSourceBase::EnableStableReadGroups()
{
    YT_VERIFY(!State_.IsInitialized());

    // The flag is declared by the final source class, which only the registry knows by the spec.
    const auto& className = GetContext()->SourceSpec->SourceClassName;
    THROW_ERROR_EXCEPTION_UNLESS(TRegistry::Get()->IsSourceReplayStable(className),
        "Source %Qv does not support stable read groups",
        className);

    StableReadGroups_ = true;
    ReplayPartialReturnsCounter_ = GetProfiler().Counter("/stable_batch/replay_partial_returns");
}

TFuture<std::optional<TOffset>> TOrderedSourceBase::DoFetchTrimmedOffset()
{
    return MakeFuture(std::optional<TOffset>());
}

NYson::TYsonString TOrderedSourceBase::CaptureReadSettings()
{
    return {};
}

void TOrderedSourceBase::ApplyReadSettings(const NYson::TYsonString& /*settings*/)
{ }

void TOrderedSourceBase::RegisterReadSettings(const TOffset& lastOffset, const NYson::TYsonString& settings)
{
    auto& memory = State_->ReadSettingsMemory;
    if (!memory) {
        memory = New<TOrderedMemory<TOffset, TStateReadSettings>>();
    }

    auto stored = TStateReadSettings{.Node = ReadSettingsToNode(settings)};
    if (!memory->empty() && !(memory->back().second == stored)) {
        YT_TLOG_INFO("Changed read settings")
            .With("LastOffset", lastOffset)
            .With("Settings", FormatReadSettings(settings));
    }

    memory->Register(lastOffset, stored);
}

void TOrderedSourceBase::OpenReadGroup()
{
    if (ReplayedGroup_ || !State_->OffsetMemory->IsRegistered(NextReadOffset_)) {
        return;
    }

    // The cursor may be inside the entry after leading payloadless records or after a trim.
    auto entryIt = std::lower_bound(
        State_->OffsetMemory->begin(),
        State_->OffsetMemory->end(),
        NextReadOffset_,
        [] (const auto& entry, const TOffset& offset) {
            return entry.first < offset;
        });

    YT_VERIFY(entryIt != State_->OffsetMemory->end());

    auto& group = ReplayedGroup_.emplace(TReplayedReadGroup{
        .EndExclusive = GetNextOffset(entryIt->first),
        .OpenInstant = TInstant::Now(),
    });

    // The settings are registered under the same last offset of the read as the group's entry.
    if (const auto& memory = State_->ReadSettingsMemory; memory && memory->IsRegistered(entryIt->first)) {
        group.Settings = ReadSettingsFromNode(memory->Extract(entryIt->first).Node);
    } else {
        // A group registered without stable read groups, for example by the existing reader.
        group.Settings = CaptureReadSettings();
        YT_TLOG_WARNING("No read settings remembered for a replayed group; using the current ones")
            .With("EntryOffset", entryIt->first)
            .With("Settings", FormatReadSettings(group.Settings));
    }

    YT_TLOG_INFO("Replaying read group")
        .With("NextReadOffset", NextReadOffset_)
        .With("EntryOffset", entryIt->first)
        .With("ReadGroupEndExclusive", group.EndExclusive)
        .With("SeqNo", entryIt->second)
        .With("Settings", FormatReadSettings(group.Settings));
}

TOffset TOrderedSourceBase::GetReadGroupNextOffset() const
{
    const auto& records = ReplayedGroup_->Records;
    return records.empty() ? NextReadOffset_ : GetNextOffset(records.back().Offset);
}

bool TOrderedSourceBase::DropStaleReadGroupRecords()
{
    // A trim or a reposition may have moved the cursor into or past the buffered records; they were
    // dropped from the source.
    if (!ReplayedGroup_) {
        return false;
    }

    EraseRecordsBefore(&ReplayedGroup_->Records, NextReadOffset_);
    if (NextReadOffset_ < ReplayedGroup_->EndExclusive) {
        return false;
    }

    YT_TLOG_INFO("Read group discarded by a cursor jump")
        .With("NextReadOffset", NextReadOffset_)
        .With("ReadGroupEndExclusive", ReplayedGroup_->EndExclusive);

    ReplayedGroup_.reset();
    return true;
}

TFuture<std::vector<ISource::TMessageBatch>> TOrderedSourceBase::ReadReplayedGroup(const TMessageBatcherSettingsPtr& settings)
{
    return BIND([weakThis = MakeWeak(this), settings, loopStart = GetCpuInstant(), Logger = Logger] {
        auto lock = [&] {
            auto strongThis = weakThis.Lock();
            if (!strongThis) {
                THROW_ERROR_EXCEPTION("Interrupted");
            }
            return strongThis;
        };

        while (true) {
            auto read = lock()->StartReplayedGroupRead(settings);
            auto records = NConcurrency::WaitFor(std::move(read.Records))
                .ValueOrThrow();
            lock()->ValidateReplayedGroupRecords(&records, read.NextOffset);
            const bool progress = !records.empty();

            // A trim that the source has not applied yet would join records from both sides of it, and a trim
            // of the whole unread tail leaves no records to show a gap.
            if (!progress || lock()->HasReadGroupGap(records)) {
                auto trimmedOffsetOrError = NConcurrency::WaitForFast(lock()->DoFetchTrimmedOffset());
                std::optional<TOffset> trimmedOffset;
                if (trimmedOffsetOrError.IsOK()) {
                    trimmedOffset = trimmedOffsetOrError.Value();
                } else if (progress || trimmedOffsetOrError.FindMatching(NYT::EErrorCode::Canceled)) {
                    trimmedOffsetOrError.ThrowOnError();
                } else {
                    // The read made no progress and is retried by the next call, as after a failed pull; the
                    // buffered records are kept.
                    YT_TLOG_WARNING("Failed to fetch trimmed offset for a replayed read group")
                        .With(TError(trimmedOffsetOrError));
                }

                if (trimmedOffset && lock()->ApplyReadGroupTrim(*trimmedOffset, &records)) {
                    return std::vector<ISource::TMessageBatch>();
                }
            }

            if (auto batches = lock()->OnReplayedGroupRecords(std::move(records), progress, settings, loopStart)) {
                return std::move(*batches);
            }
        }
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

TOrderedSourceBase::TReplayedGroupRead TOrderedSourceBase::StartReplayedGroupRead(const TMessageBatcherSettingsPtr& settings)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);
    YT_VERIFY(ReplayedGroup_);

    auto nextOffset = GetReadGroupNextOffset();
    ++ReplayedGroup_->ReadCalls;
    ApplyReadSettings(ReplayedGroup_->Settings);
    auto records = DoReadRange(settings, nextOffset, ReplayedGroup_->EndExclusive);
    return {.NextOffset = std::move(nextOffset), .Records = std::move(records)};
}

bool TOrderedSourceBase::HasReadGroupGap(const std::vector<TRecord>& records) const
{
    // A gap before the first buffered record leaves nothing to drop.
    std::optional<TOffset> previous;
    if (!ReplayedGroup_->Records.empty()) {
        previous = ReplayedGroup_->Records.back().Offset;
    }

    for (const auto& record : records) {
        if (previous && GetNextOffset(*previous) < record.Offset) {
            return true;
        }

        previous = record.Offset;
    }

    return false;
}

bool TOrderedSourceBase::ApplyReadGroupTrim(const TOffset& trimmedOffset, std::vector<TRecord>* records)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    // Otherwise the gap is in the data itself, and the records stay as read.
    if (trimmedOffset <= NextReadOffset_) {
        return false;
    }

    YT_TLOG_INFO("Trim found inside a replayed read group")
        .With("NextReadOffset", NextReadOffset_)
        .With("TrimmedOffset", trimmedOffset)
        .With("ReadGroupEndExclusive", ReplayedGroup_->EndExclusive);

    // Applied as #FlushDelayedPartitionInfoUpdates applies a reported trim. The cursor stops at the group
    // end: the connector may already hold records past it, which are read normally.
    TryIncreaseMaxOffsetExclusive(trimmedOffset, false);
    NextReadOffset_ = std::min(trimmedOffset, ReplayedGroup_->EndExclusive);
    MarkMissingMessagesPersisted();

    EraseRecordsBefore(records, NextReadOffset_);

    if (DropStaleReadGroupRecords()) {
        CleanUpInflightOffsets();
        return true;
    }

    return false;
}

std::optional<std::vector<ISource::TMessageBatch>> TOrderedSourceBase::OnReplayedGroupRecords(
    std::vector<TRecord>&& records,
    bool progress,
    const TMessageBatcherSettingsPtr& settings,
    TCpuInstant loopStart)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);
    YT_VERIFY(ReplayedGroup_);

    auto& buffer = ReplayedGroup_->Records;
    buffer.insert(
        buffer.end(),
        std::make_move_iterator(records.begin()),
        std::make_move_iterator(records.end()));

    if (GetReadGroupNextOffset() >= ReplayedGroup_->EndExclusive) {
        return TakeReadGroup();
    }

    // Waiting for the rest inside this call would hold commits and interruption while the connector
    // has nothing to return, or progresses slowly.
    if (!progress) {
        return ReturnIncompleteReadGroup("no_progress");
    }

    if (GetCpuInstant() - loopStart >= DurationToCpuDuration(settings->BatchDuration)) {
        return ReturnIncompleteReadGroup("time_bound");
    }

    return std::nullopt;
}

std::vector<ISource::TMessageBatch> TOrderedSourceBase::ReturnIncompleteReadGroup(TStringBuf reason)
{
    auto& group = *ReplayedGroup_;
    ++group.PartialReturns;
    ReplayPartialReturnsCounter_.Increment();

    const bool notable = group.PartialReturns == 1 || group.PartialReturns % 100 == 0;

    YT_TLOG_EVENT(Logger, notable ? NLogging::ELogLevel::Info : NLogging::ELogLevel::Debug, "Replayed read group is incomplete; returning nothing")
        .With("Reason", reason)
        .With("NextOffset", GetReadGroupNextOffset())
        .With("ReadGroupEndExclusive", group.EndExclusive)
        .With("BufferedRecords", group.Records.size())
        .With("ReadCalls", group.ReadCalls)
        .With("PartialReturns", group.PartialReturns)
        .With("SinceOpened", TInstant::Now() - group.OpenInstant);

    CleanUpInflightOffsets();
    return {};
}

std::vector<ISource::TMessageBatch> TOrderedSourceBase::TakeReadGroup()
{
    // A trim inside the group or a reposition may have moved the cursor while the group was read.
    if (DropStaleReadGroupRecords()) {
        CleanUpInflightOffsets();
        return {};
    }

    auto group = *std::exchange(ReplayedGroup_, std::nullopt);

    const auto recordCount = std::ssize(group.Records);
    auto batches = PrepareMessages(std::move(group.Records));

    YT_TLOG_INFO("Read replayed group")
        .With("Records", recordCount)
        .With("ReadCalls", group.ReadCalls)
        .With("ReadGroupEndExclusive", group.EndExclusive);

    return batches;
}

void TOrderedSourceBase::DropRecordsBeforeReadStart(std::vector<TRecord>* records, const TOffset& start)
{
    // A connector may ignore the read limits. Records before the read start were read already.
    if (auto dropped = EraseRecordsBefore(records, start)) {
        YT_TLOG_DEBUG("Dropped records before the read start")
            .With("DroppedRecords", dropped)
            .With("ReadStart", start);
    }
}

void TOrderedSourceBase::ValidateReplayedGroupRecords(std::vector<TRecord>* records, const TOffset& start)
{
    DropRecordsBeforeReadStart(records, start);

    if (!records->empty() && records->back().Offset >= ReplayedGroup_->EndExclusive) {
        THROW_ERROR_EXCEPTION("Source returned record %v past the end %v of the replayed read group",
            records->back().Offset,
            ReplayedGroup_->EndExclusive);
    }
}

bool TOrderedSourceBase::IsFinite()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);
    return GetParameters()->Finite;
}

void TOrderedSourceBase::UpdateUnavailability()
{
    NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    bool anyKnown = false;
    // The instant the current failure run began: the earliest flip among the states failing right now.
    auto brokenSince = TInstant::Max();
    // Only states that have a verdict may vote; one that has never spoken would otherwise pin this to
    // zero. Taking the minimum is what keeps a healthy state from vouching for a failing one.
    auto lastOKTime = TInstant::Max();
    for (const auto& errorState : AvailabilityErrorStates_) {
        auto status = errorState->GetStatus();
        if (!status.IsOK.has_value()) {
            continue;
        }
        anyKnown = true;
        lastOKTime = std::min(lastOKTime, status.LastOKTime);
        if (!status.IsOK.value()) {
            brokenSince = std::min(brokenSince, status.LastStateChangeTime);
        }
    }

    // Nobody has a verdict: the state is unknown, so neither charge nor reset. This is what lets a
    // freshly started instance keep reporting the unavailability it inherited until the source speaks.
    if (!anyKnown) {
        return;
    }

    if (brokenSince == TInstant::Max()) {
        State_->ObservedUnavailableDuration = TDuration::Zero();
        State_->LastUnavailableInstant = TInstant::Zero();
        return;
    }

    // Every state was OK at some point after the last failure we recorded, so that failure run has
    // ended and this is a new one. Restarts do not trip this: a fresh instance has never been OK.
    if (lastOKTime > State_->LastUnavailableInstant) {
        State_->ObservedUnavailableDuration = TDuration::Zero();
        State_->LastUnavailableInstant = TInstant::Zero();
    }

    // Charging from the later of the two is what makes this observed time: a failure recorded before a
    // restart is carried over, but the gap in which no instance was watching is charged to nobody.
    const auto now = TInstant::Now();
    State_->ObservedUnavailableDuration += now - std::max(State_->LastUnavailableInstant, brokenSince);
    State_->LastUnavailableInstant = now;
}

// Mutes the whole source subtree, so every leaf the connector reports through goes quiet at once. The
// states keep recording: the very verdict that mutes them is derived from them.
void TOrderedSourceBase::UpdateStatusProfilerMute()
{
    GetContext()->StatusProfiler->Mute(GetDynamicContext()->AvailabilityGroupUnavailable);
}

TInflightStreamTraverseDataPtr TOrderedSourceBase::BuildInflight()
{
    UpdateUnavailability();
    FlushDelayedPartitionInfoUpdates();

    auto inflight = New<TInflightStreamTraverseData>();

    inflight->MinSystemTimestamp = std::max(State_->LastPersistedWriteTimestamp, State_->FirstNotPersistedWriteTimestamp);
    inflight->MinEventTimestamp = GetPersistedEventWatermark();
    const bool wasEmptyRecently = State_->CommittedOffsetExclusive == State_->MaxOffsetExclusive &&
        State_->MaxOffsetIsConfirmed;
    inflight->Empty = wasEmptyRecently && IsFinite();
    inflight->Suspended = State_->CommittedOffsetExclusive == State_->PublishedOffsetExclusive &&
        (IsDraining() || inflight->Empty);

    const i64 offsetLag = DoGetEstimatedRowsAtOffset(State_->MaxOffsetExclusive) - DoGetEstimatedRowsAtOffset(State_->CommittedOffsetExclusive);
    if (offsetLag == 0) {
        inflight->InflightMetrics->Count = 0;
    } else {
        inflight->InflightMetrics->Count = std::max<i64>(1, offsetLag * State_->AvgOffsetCountSize);
    }
    inflight->InflightMetrics->ByteSize = offsetLag * State_->AvgOffsetByteSize;

    const i64 readyOffsetLag =
        DoGetEstimatedRowsAtOffset(State_->MaxOffsetExclusive) - DoGetEstimatedRowsAtOffset(NextReadOffset_);
    inflight->InflightMetrics->ReadyCount = readyOffsetLag == 0
        ? 0
        : std::max<i64>(1, readyOffsetLag * State_->AvgOffsetCountSize);
    inflight->InflightMetrics->ReadyByteSize = readyOffsetLag * State_->AvgOffsetByteSize;

    if (inflight->InflightMetrics->Count == 0 && State_->LastIdleInstant > State_->LastNotIdleInstant) {
        inflight->InflightMetrics->IdleDuration = State_->LastIdleInstant - State_->LastNotIdleInstant;
        inflight->InflightMetrics->LastIdleTimestamp = TSystemTimestamp(State_->LastIdleInstant.Seconds());
    }

    if (SourceMaxOffsetObserved_) {
        inflight->InflightMetrics->NewCountPerSec = SourceTotalCount_.GetLastRate();
        inflight->InflightMetrics->NewBytesPerSec = SourceTotalBytes_.GetLastRate();
    }
    if (auto backlogRate = EstimateBacklogRate()) {
        inflight->InflightMetrics->NewCountPerSec = std::max(
            inflight->InflightMetrics->NewCountPerSec.value_or(0),
            std::max(0.0, backlogRate->MessagesPerSecond));
        inflight->InflightMetrics->NewBytesPerSec = std::max(
            inflight->InflightMetrics->NewBytesPerSec.value_or(0),
            std::max(0.0, backlogRate->BytesPerSecond));
    }
    inflight->InflightMetrics->OfferedCountPerSec = inflight->InflightMetrics->NewCountPerSec;
    inflight->InflightMetrics->OfferedBytesPerSec = inflight->InflightMetrics->NewBytesPerSec;
    inflight->InflightMetrics->ProcessedCountPerSec = PersistedCount_.GetDecayedRate();
    inflight->InflightMetrics->ProcessedBytesPerSec = PersistedBytes_.GetDecayedRate();

    if (State_->LastUnavailableInstant) {
        const auto threshold = GetDynamicParameters()->UnavailableThreshold;
        const bool isStablyUnavailable = (State_->ObservedUnavailableDuration > threshold);
        YT_TLOG_DEBUG("Source is unavailable right now")
            .With("LastUnavailableInstant", State_->LastUnavailableInstant)
            .With("UnavailableThreshold", threshold)
            .With("ObservedUnavailableDuration", State_->ObservedUnavailableDuration)
            .With("IsStablyUnavailable", isStablyUnavailable);
        if (isStablyUnavailable) {
            inflight->InflightMetrics->UnavailableTimestamp = TSystemTimestamp(State_->LastUnavailableInstant.Seconds());
        }
    }

    YT_VERIFY(inflight->InflightMetrics->Count >= 0);
    AdjustInflight(inflight);
    YT_TLOG_INFO("Built source inflight")
        .With("Inflight", NYson::ConvertToYsonString(inflight, NYson::EYsonFormat::Text))
        .With("State", NYson::ConvertToYsonString(State_, NYson::EYsonFormat::Text));
    return inflight;
}

void TOrderedSourceBase::AdjustInflight(const TInflightStreamTraverseDataPtr& /*inflight*/)
{ }

std::optional<TSystemTimestamp> TOrderedSourceBase::GetPersistedEventWatermark()
{
    return State_->PersistedEventWatermark;
}

std::optional<TSystemTimestamp> TOrderedSourceBase::GetReadEventWatermark()
{
    return ReadEventWatermark_;
}

TDuration TOrderedSourceBase::GetAlignmentTimestampWindow()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    if (InflightOffsets_.empty()) {
        return TDuration::Zero();
    }

    const auto firstInstant = TInstant::Seconds(InflightOffsets_.front().AlignmentTimestamp.Underlying());
    const auto lastInstant = TInstant::Seconds(LastReadAlignmentTimestamp_.Underlying());

    if (lastInstant <= firstInstant) {
        return TDuration::Zero();
    }

    return lastInstant - firstInstant;
}

TSystemTimestamp TOrderedSourceBase::GetReadAlignmentTimestamp()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    if (LastReadAlignmentTimestamp_ != ZeroSystemTimestamp) {
        return LastReadAlignmentTimestamp_;
    }
    if (State_.IsInitialized()) {
        if (!State_->AlignmentTimestampMemory->empty()) {
            return State_->AlignmentTimestampMemory->front().second;
        }
        return State_->MaxMemorizedAlignmentTimestamp;
    }
    return ZeroSystemTimestamp;
}

std::vector<ISource::TMessageBatch> TOrderedSourceBase::PrepareMessages(std::vector<TRecord>&& records)
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    if (records.empty()) {
        CleanUpInflightOffsets();
        return {};
    }

    std::vector<TOffset> nextOffsets;
    nextOffsets.reserve(records.size());
    for (i64 recordIndex = 0; recordIndex < std::ssize(records); ++recordIndex) {
        const auto& nextOffset = nextOffsets.emplace_back(GetNextOffset(records[recordIndex].Offset));
        YT_TLOG_FATAL_UNLESS(records[recordIndex].Offset < nextOffset,
            "Next record offset must advance")
            .With("RecordOffset", records[recordIndex].Offset)
            .With("NextOffset", nextOffset);
        if (recordIndex + 1 < std::ssize(records)) {
            YT_TLOG_FATAL_UNLESS(nextOffset <= records[recordIndex + 1].Offset,
                "Record offsets must be ordered and non-overlapping")
                .With("RecordOffset", records[recordIndex].Offset)
                .With("NextOffset", nextOffset)
                .With("FollowingRecordOffset", records[recordIndex + 1].Offset);
        }
    }

    auto seqNoProviderResult = NConcurrency::WaitFor(GetContext()->TimeProvider->GenerateGlobalUniqueSeqNo()).ValueOrThrow();
    YT_TLOG_FATAL_UNLESS(records.front().Offset >= NextReadOffset_,
        "Record offset must not precede the next read offset")
        .With("RecordOffset", records.front().Offset)
        .With("NextReadOffset", NextReadOffset_);
    YT_TLOG_FATAL_UNLESS(records.front().Offset >= State_->PersistedOffsetExclusive,
        "Record offset must not precede the persisted boundary")
        .With("RecordOffset", records.front().Offset)
        .With("PersistedOffsetExclusive", State_->PersistedOffsetExclusive);

    auto minWriteTimestamp = InfinitySystemTimestamp;
    for (auto& record : records) {
        minWriteTimestamp = std::min(minWriteTimestamp, record.WriteTimestamp);
        if (GetParameters()->WriteTimeColumn && !MaterializesWriteTimeColumn() && !record.Payloads.empty()) {
            AppendWriteTime(record);
        }
    }

    std::vector<ISource::TMessageBatch> parsedMessages;
    for (i64 recordIndex = 0; recordIndex < std::ssize(records); ++recordIndex) {
        auto& record = records[recordIndex];
        NextReadOffset_ = nextOffsets[recordIndex];
        if (record.Meta && record.Meta->EventWatermark) {
            ReadEventWatermark_ = std::max(ReadEventWatermark_, record.Meta->EventWatermark);
        }

        State_->OffsetMemory->Register(record.Offset, seqNoProviderResult.UniqueSeqNo);
        auto extractedUniqueSeqNo = State_->OffsetMemory->Extract(record.Offset);
        YT_TLOG_FATAL_UNLESS(seqNoProviderResult.UniqueSeqNo >= extractedUniqueSeqNo,
            "Expected that new generated unique seq no is greater than persisted")
            .With("NewGeneratedUniqueSeqNo", seqNoProviderResult.UniqueSeqNo)
            .With("PersistedUniqueSeqNo", extractedUniqueSeqNo);
        auto alignmentTimestamp = [&] {
            if (State_->AlignmentTimestampMemory->IsRegistered(extractedUniqueSeqNo)) {
                return State_->AlignmentTimestampMemory->Extract(extractedUniqueSeqNo);
            }
            auto candidateTimestamp = std::min(seqNoProviderResult.Timestamp, minWriteTimestamp); // Limit external timestamp with fresh generated timestamp.
            State_->MaxMemorizedAlignmentTimestamp = std::max(State_->MaxMemorizedAlignmentTimestamp, candidateTimestamp);
            State_->AlignmentTimestampMemory->Register(extractedUniqueSeqNo, State_->MaxMemorizedAlignmentTimestamp);
            return State_->MaxMemorizedAlignmentTimestamp;
        }();
        LastReadAlignmentTimestamp_ = std::max(LastReadAlignmentTimestamp_, alignmentTimestamp);


        // Store seqNo only — TMessageId is generated lazily at drain time (see TOffsetInfo).
        if ((record.Meta && record.Meta->PureHeartbeat) || record.Payloads.empty()) {
            InflightOffsets_.emplace_back() = TOffsetInfo{
                .Offset = record.Offset,
                // Heartbeats and malformed records with no payloads carry no messages — mark as persisted immediately.
                .State = EMessageState::Persisted,
                .NextMessageSeqNo = extractedUniqueSeqNo,
                .NextOffset = NextReadOffset_,
                .WriteTimestamp = record.WriteTimestamp,
                .AlignmentTimestamp = alignmentTimestamp,
                .EventWatermark = record.Meta
                    ? record.Meta->EventWatermark
                    : std::nullopt,
                .Count = 0,
                .ByteSize = 0,
            };
        } else {
            InflightOffsets_.emplace_back() = TOffsetInfo{
                .Offset = record.Offset,
                .NextMessageSeqNo = extractedUniqueSeqNo,
                .NextOffset = NextReadOffset_,
                .WriteTimestamp = record.WriteTimestamp,
                .AlignmentTimestamp = alignmentTimestamp,
                .EventWatermark = record.Meta
                    ? record.Meta->EventWatermark
                    : std::nullopt,
                .Count = std::ssize(record.Payloads),
                .ByteSize = 0, // Will be modified later.
            };
            auto offsetInfoIt = std::prev(InflightOffsets_.end());

            std::vector<TInputMessageConstPtr> messages;
            messages.reserve(record.Payloads.size());
            for (i64 i = 0; i < std::ssize(record.Payloads); ++i) {
                const auto& payload = record.Payloads[i];
                TMessage message;
                message.MessageId = GenerateOrderedMessageId(extractedUniqueSeqNo, GetContext()->SourceStreamId, ConvertOffsetToLexicographicallyComparableString(record.Offset), LexicographicallySerialize(i));
                if (record.WriteTimestamp == ZeroSystemTimestamp) {
                    THROW_ERROR_EXCEPTION("WriteTimestamp is undefined")
                        .With("stream_id", GetContext()->SourceStreamId)
                        .With("message_id", message.MessageId);
                }
                message.SystemTimestamp = record.WriteTimestamp;
                message.AlignmentTimestamp = alignmentTimestamp;

                if (record.CreateTimestamp == ZeroSystemTimestamp) {
                    THROW_ERROR_EXCEPTION("EventTimestamp is undefined")
                        .With("stream_id", GetContext()->SourceStreamId)
                        .With("message_id", message.MessageId);
                }

                message.EventTimestamp = record.CreateTimestamp;
                if (record.Meta) {
                    ApplyFlowQueueMeta(*record.Meta, message, i);
                }

                message.StreamId = GetContext()->SourceStreamId;
                message.PayloadSchema = record.PayloadSchema;
                message.Payload = payload;

                YT_TLOG_DEBUG("MessageLifeCycle.Source: message was received")
                    .With("MessageId", message.MessageId)
                    .With("StreamId", message.StreamId)
                    .With("SystemTimestamp", message.SystemTimestamp)
                    .With("EventTimestamp", message.EventTimestamp)
                    .With("Offset", record.Offset);

                auto inputMessage = New<TInputMessage>(std::move(message), GetContext()->SourceKey);
                offsetInfoIt->ByteSize += inputMessage->ByteSize;
                messages.push_back(std::move(inputMessage));
            }

            auto alpha = GetParameters()->ByteSizeAlpha;
            State_->AvgOffsetByteSize = State_->AvgOffsetByteSize * (1. - alpha) + alpha * offsetInfoIt->ByteSize;
            State_->AvgOffsetCountSize = State_->AvgOffsetCountSize * (1. - alpha) + alpha * std::size(record.Payloads);

            parsedMessages.push_back(ISource::TMessageBatch{
                .Cookie = TSourceMessageBatchCookie(std::any(TMessageCookieData{.OffsetInfoIt = offsetInfoIt})),
                .Messages = std::move(messages),
                .OriginalStatistics = TBatchStatistics{.Count = offsetInfoIt->Count, .ByteSize = offsetInfoIt->ByteSize},
            });
        }
        TryCollapseOffsetInfo(std::prev(InflightOffsets_.end()));
    }
    State_->LastNotIdleInstant = TInstant::Now();
    State_->LastIdleInstant = TInstant::Zero();
    TryIncreaseMaxOffsetExclusive(NextReadOffset_, false);
    CleanUpInflightOffsets();
    return parsedMessages;
}

////////////////////////////////////////////////////////////////////////////////

TOffset TIntegerOffsetOrderedSourceBase::GetNextOffset(const TOffset& offset) const
{
    return IntToOffset(OffsetToInt(offset) + 1);
}

std::string TIntegerOffsetOrderedSourceBase::ConvertOffsetToLexicographicallyComparableString(const TOffset& offset) const
{
    return LexicographicallySerialize(OffsetToInt(offset));
}

bool TIntegerOffsetOrderedSourceBase::AreOffsetsConsecutive() const
{
    return true;
}

bool TIntegerOffsetOrderedSourceBase::CanCommittedOffsetExceedNextReadOffset() const
{
    return false;
}

i64 TIntegerOffsetOrderedSourceBase::DoGetEstimatedRowsAtOffset(TOffset offset)
{
    return OffsetToInt(offset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
