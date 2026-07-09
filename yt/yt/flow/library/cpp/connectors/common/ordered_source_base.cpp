#include "ordered_source_base.h"

#include <yt/yt/flow/library/cpp/common/seq_no_provider.h>
#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/transaction_client/helpers.h>

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

} // namespace

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
        "Committed offset is greater than persisted offset (CommittedOffsetExclusive: %v, PersistedOffsetExclusive: %v)",
        CommittedOffsetExclusive,
        PersistedOffsetExclusive);
    THROW_ERROR_EXCEPTION_UNLESS(PersistedOffsetExclusive <= PublishedOffsetExclusive,
        "Persisted offset is greater than published offset (PersistedOffsetExclusive: %v, PublishedOffsetExclusive: %v)",
        PersistedOffsetExclusive,
        PublishedOffsetExclusive);
    THROW_ERROR_EXCEPTION_UNLESS(PublishedOffsetExclusive <= MaxOffsetExclusive,
        "Published offset is greater than max offset (PublishedOffsetExclusive: %v, MaxOffsetExclusive: %v)",
        PublishedOffsetExclusive,
        MaxOffsetExclusive);
    THROW_ERROR_EXCEPTION_UNLESS(LastIdleInstant >= LastNotIdleInstant || LastIdleInstant == TInstant::Zero(),
        "Strange value of LastIdleInstant (LastIdleInstant: %v, LastNotIdleInstant: %v)",
        LastIdleInstant,
        LastNotIdleInstant);
}

void TOrderedSourcePartitionState::SyncObsoleteOffsets()
{
    CommittedOffsetExclusiveObsolete = OffsetToInt(CommittedOffsetExclusive);
    PersistedOffsetExclusiveObsolete = OffsetToInt(PersistedOffsetExclusive);
    PublishedOffsetExclusiveObsolete = OffsetToInt(PublishedOffsetExclusive);
    MaxOffsetExclusiveObsolete = OffsetToInt(MaxOffsetExclusive);

    OffsetMemoryObsolete = New<TOrderedMemory<i64, TUniqueSeqNo>>();
    for (const auto& [offset, seqNo] : *OffsetMemory) {
        OffsetMemoryObsolete->Register(OffsetToInt(offset), seqNo);
    }
}

void TOrderedSourcePartitionState::Register(TRegistrar registrar)
{
    registrar.Parameter("committed_offset_exclusive", &TThis::CommittedOffsetExclusiveObsolete)
        .Default();
    registrar.Parameter("persisted_offset_exclusive", &TThis::PersistedOffsetExclusiveObsolete)
        .Default();
    registrar.Parameter("published_offset_exclusive", &TThis::PublishedOffsetExclusiveObsolete)
        .Default();
    registrar.Parameter("max_offset_exclusive", &TThis::MaxOffsetExclusiveObsolete)
        .Default();

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
    registrar.Parameter("decayed_unavailable_duration", &TThis::DecayedUnavailableDuration)
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
    registrar.Parameter("offset_memory", &TThis::OffsetMemoryObsolete)
        .Default();

    registrar.Parameter("alignment_timestamp_memory", &TThis::AlignmentTimestampMemory)
        .DefaultNew();
    registrar.Parameter("max_memorized_alignment_timestamp", &TThis::MaxMemorizedAlignmentTimestamp)
        .Default(ZeroSystemTimestamp);

    registrar.Postprocessor([] (TThis* state) {
        if (state->CommittedOffsetExclusiveObsolete) {
            state->CommittedOffsetExclusive = IntToOffset(*state->CommittedOffsetExclusiveObsolete);
        }

        if (state->PersistedOffsetExclusiveObsolete) {
            state->PersistedOffsetExclusive = IntToOffset(*state->PersistedOffsetExclusiveObsolete);
        }

        if (state->PublishedOffsetExclusiveObsolete) {
            state->PublishedOffsetExclusive = IntToOffset(*state->PublishedOffsetExclusiveObsolete);
        }

        if (state->MaxOffsetExclusiveObsolete) {
            state->MaxOffsetExclusive = IntToOffset(*state->MaxOffsetExclusiveObsolete);
        }

        if (state->OffsetMemoryObsolete) {
            state->OffsetMemory = New<TOrderedMemory<TOffset, TUniqueSeqNo>>();
            for (const auto& [offset, seqNo] : *state->OffsetMemoryObsolete) {
                state->OffsetMemory->Register(IntToOffset(offset), seqNo);
            }
        }
    });
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
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedSourceBase::TExtendedDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("unavailable_time_half_decay_period", &TThis::UnavailableTimeHalfDecayPeriod)
        .Default(TDuration::Minutes(10));
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedSourceBase::TryIncreaseMaxOffsetExclusive(TOffset newMaxOffsetExclusive, bool confirmed)
{
    if (newMaxOffsetExclusive > State_->MaxOffsetExclusive) {
        State_->MaxOffsetExclusive = newMaxOffsetExclusive;
        State_->MaxOffsetIsConfirmed = confirmed;
        i64 deltaRows = DoGetEstimatedRowsAtOffset(newMaxOffsetExclusive) - DoGetEstimatedRowsAtOffset(State_->MaxOffsetExclusive);
        SourceTotalCount_.Inc(deltaRows * State_->AvgOffsetCountSize);
        SourceTotalBytes_.Inc(deltaRows * State_->AvgOffsetByteSize);
    } else if (newMaxOffsetExclusive == State_->MaxOffsetExclusive) {
        State_->MaxOffsetIsConfirmed |= confirmed;
    }
}

TOrderedSourceBase::TOrderedSourceBase(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TSourceBase(std::move(context), std::move(dynamicContext))
    , Logger(TSourceBase::Logger)
    , NextReadOffset_(MakeKey())
    , CommittedOffsetExclusive_(MakeKey())
    , ReadErrorState_(GetContext()->StatusProfiler->ErrorState("/read"))
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

    NextReadOffset_ = State_->PersistedOffsetExclusive;
    ReadEventWatermark_ = State_->PersistedEventWatermark;
    CommittedOffsetExclusive_ = State_->CommittedOffsetExclusive;
    DoReportPersistedOffset(State_->PersistedOffsetExclusive);
    DoInit();

    SourceTotalCount_.Update(0);
    SourceTotalBytes_.Update(0);

    InitInstant_ = TInstant::Now();
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
        static const ui64 minPossibleSeqNo = NTransactionClient::TimestampFromUnixTime(TInstant::ParseIso8601("2025-01-01").Seconds());
        ui64 lastSeqNo = minPossibleSeqNo;
        for (auto& [offset, seqNo] : *State_->OffsetMemory) {
            YT_VERIFY(offset >= lastOffset);
            YT_VERIFY(seqNo.Underlying() >= lastSeqNo);
            lastOffset = offset;
            lastSeqNo = seqNo.Underlying();
        }
        YT_VERIFY(lastOffset < State_->MaxOffsetExclusive);
    }

    //! This is inherently temporary code for two-way compatibility with original (int) offsets.
    if (dynamic_cast<TIntegerOffsetOrderedSourceBase*>(this)) {
        State_->SyncObsoleteOffsets();
    }
}

void TOrderedSourceBase::Commit()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    YT_LOG_INFO("Persisted state (State: %v, NextOffset: %v)",
        NYson::ConvertToYsonString(State_, NYson::EYsonFormat::Text),
        NextReadOffset_);

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

IStatusErrorStatePtr TOrderedSourceBase::GetReadErrorState() const
{
    return ReadErrorState_;
}

const NProfiling::TProfiler& TOrderedSourceBase::GetProfiler() const
{
    return Profiler_;
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

            YT_LOG_DEBUG("Apply partition info update (CommittedOffsetExclusive: %v, MaxOffsetExclusive: %v)",
                update.CommittedOffsetExclusive,
                update.MaxOffsetExclusive);

            if (update.CommittedOffsetExclusive) {
                CommittedOffsetExclusive_ = std::max(CommittedOffsetExclusive_, *update.CommittedOffsetExclusive);
                TryIncreaseMaxOffsetExclusive(*update.CommittedOffsetExclusive, false);
            }
            if (CommittedOffsetExclusive_ > NextReadOffset_) {
                if (!CanCommittedOffsetExceedNextReadOffset()) {
                    YT_LOG_ERROR("Rewind NextReadOffset up to CommittedOffsetExclusive. Probably some input data was trimmed before reading "
                        "(NewNextReadOffset: %v, OldNextReadOffset: %v)",
                        CommittedOffsetExclusive_,
                        NextReadOffset_);
                }
                NextReadOffset_ = CommittedOffsetExclusive_;
                MarkMissingMessagesPersisted();
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
            YT_LOG_FATAL(ex, "Fatal error in updating partition info");
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
        YT_LOG_FATAL_UNLESS(persistedMessageIdExclusive >= State_->PersistedMessageIdExclusive,
            "Expected that new persisted message id is greater than persisted in state "
            "(NewPersistedMessageIdExclusive: %v, StatePersistedMessageIdExclusive: %v)",
            persistedMessageIdExclusive,
            State_->PersistedMessageIdExclusive);
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

    State_->OffsetMemory->AdvanceExclusive(State_->PersistedOffsetExclusive);
    MarkMissingMessagesPersisted();
}

void TOrderedSourceBase::MarkMissingMessagesPersisted()
{
    YT_VERIFY(GetCurrentInvoker() == GetContext()->SerializedInvoker);

    const auto& firstNotPersistedMessageOffset = InflightOffsets_.empty() ? NextReadOffset_ : InflightOffsets_.front().Offset;

    YT_LOG_FATAL_UNLESS(firstNotPersistedMessageOffset >= State_->PersistedOffsetExclusive,
        "Expected increasing of offsets (NewPersistedOffsetExclusive: %v, OldPersistedOffsetExclusive: %v)",
        firstNotPersistedMessageOffset,
        State_->PersistedOffsetExclusive);
    if (firstNotPersistedMessageOffset > State_->PersistedOffsetExclusive) {
        if (AreOffsetsConsecutive()) {
            YT_LOG_ERROR("Rewind PersistedOffsetExclusive up to first not persisted message offset. "
                "Probably some input data was trimmed before reading "
                "(OldPersistedOffsetExclusive: %v, FirstNotPersistedMessageOffset: %v)",
                State_->PersistedOffsetExclusive,
                firstNotPersistedMessageOffset);
        }
        State_->PersistedOffsetExclusive = firstNotPersistedMessageOffset;
        State_->PublishedOffsetExclusive = std::max(State_->PublishedOffsetExclusive, State_->PersistedOffsetExclusive);
        State_->EnsureInvariants();
        State_->OffsetMemory->AdvanceExclusive(State_->PersistedOffsetExclusive);

        TUniqueSeqNo seqNoWatermark = {};
        if (State_->OffsetMemory->empty()) {
            seqNoWatermark = NConcurrency::WaitFor(GetContext()->UniqueSeqNoProvider->Generate()).ValueOrThrow().UniqueSeqNo;
        } else {
            seqNoWatermark = State_->OffsetMemory->front().second;
        }

        const TMessageId newPersistedMessageIdExclusive = GenerateOrderedMessageId(
            seqNoWatermark,
            GetContext()->SourceStreamId,
            ConvertOffsetToLexicographicallyComparableString(firstNotPersistedMessageOffset));
        YT_LOG_FATAL_UNLESS(newPersistedMessageIdExclusive >= State_->PersistedMessageIdExclusive,
            "Expected increasing of message ids (NewPersistedMessageId: %v, OldPersistedMessageId: %v)",
            newPersistedMessageIdExclusive,
            State_->PersistedMessageIdExclusive);
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

    std::optional<TOffset> offsetLimitExclusive;
    if (IsDraining()) {
        if (NextReadOffset_ >= State_->PublishedOffsetExclusive) {
            return MakeFuture<std::vector<ISource::TMessageBatch>>({});
        } else {
            offsetLimitExclusive = State_->PublishedOffsetExclusive;
        }
    }
    YT_VERIFY(NextReadOffset_ >= State_->PersistedOffsetExclusive);
    return DoReadNextBatch(batcherSettings, NextReadOffset_, offsetLimitExclusive)
        .AsUnique()
        .Apply(BIND([weakThis = MakeWeak(this)] (TErrorOr<std::vector<TRecord>>&& result) {
            if (auto strongThis = weakThis.Lock()) {
                return strongThis->PrepareMessages(std::move(result.ValueOrThrow()));
            }
            THROW_ERROR_EXCEPTION("Interrupted");
        })
                .AsyncVia(GetCurrentInvoker()));
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
    auto errorOwnerState = ReadErrorState_->GetStatus();
    const auto now = TInstant::Now();
    if (errorOwnerState.IsOK.has_value()) {
        if (errorOwnerState.IsOK.value()) {
            State_->DecayedUnavailableDuration = TDuration::Zero();
            State_->LastUnavailableInstant = TInstant::Zero();
        } else {
            if (errorOwnerState.LastOKTime > State_->LastUnavailableInstant) {
                State_->DecayedUnavailableDuration = TDuration::Zero();
                State_->LastUnavailableInstant = TInstant::Zero();
            }
            const auto lastUnavailableInstant = (State_->LastUnavailableInstant ? State_->LastUnavailableInstant : errorOwnerState.LastStateChangeTime);
            const auto halfDecayPeriod = GetDynamicParameters()->UnavailableTimeHalfDecayPeriod;
            const auto failDuration = now - std::max(lastUnavailableInstant, InitInstant_);
            // Decaying of continuous range.
            const auto delta = halfDecayPeriod / std::log(2.) * (1. - std::pow(0.5, failDuration / halfDecayPeriod));
            State_->DecayedUnavailableDuration = delta +
                State_->DecayedUnavailableDuration * std::pow(0.5, (now - State_->LastUnavailableInstant) / halfDecayPeriod);
            State_->LastUnavailableInstant = now;
        }
    }
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

    if (inflight->InflightMetrics->Count == 0 && State_->LastIdleInstant > State_->LastNotIdleInstant) {
        inflight->InflightMetrics->IdleDuration = State_->LastIdleInstant - State_->LastNotIdleInstant;
        inflight->InflightMetrics->LastIdleTimestamp = TSystemTimestamp(State_->LastIdleInstant.Seconds());
    }

    inflight->InflightMetrics->NewCountPerSec = SourceTotalCount_.GetRate().value_or(0);
    inflight->InflightMetrics->NewBytesPerSec = SourceTotalBytes_.GetRate().value_or(0);
    inflight->InflightMetrics->ProcessedCountPerSec = PersistedCount_.GetRate().value_or(0);
    inflight->InflightMetrics->ProcessedBytesPerSec = PersistedBytes_.GetRate().value_or(0);

    if (State_->LastUnavailableInstant) {
        const auto halfDecayPeriod = GetDynamicParameters()->UnavailableTimeHalfDecayPeriod;
        // Maximum value of DecayedUnavailableDuration is halfDecayPeriod / ln(2) ~ 1.44 * halfDecayPeriod.
        const bool isStablyUnavailable = (State_->DecayedUnavailableDuration / halfDecayPeriod > 0.5);
        YT_LOG_DEBUG("Source is unavailable right now (LastUnavailableInstant: %v, "
            "HalfDecayPeriod: %v, DecayedUnavailableDuration: %v, IsStablyUnavailable: %v)",
            State_->LastUnavailableInstant,
            halfDecayPeriod.SecondsFloat(),
            State_->DecayedUnavailableDuration.SecondsFloat(),
            isStablyUnavailable);
        if (isStablyUnavailable) {
            inflight->InflightMetrics->UnavailableTimestamp = TSystemTimestamp(State_->LastUnavailableInstant.Seconds());
        }
    }

    YT_VERIFY(inflight->InflightMetrics->Count >= 0);
    YT_LOG_INFO("Built source inflight (Inflight: %v, State: %v)",
        NYson::ConvertToYsonString(inflight, NYson::EYsonFormat::Text),
        NYson::ConvertToYsonString(State_, NYson::EYsonFormat::Text));
    return inflight;
}

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

    const bool empty = records.empty();

    auto seqNoProviderResult = NConcurrency::WaitFor(GetContext()->UniqueSeqNoProvider->Generate()).ValueOrThrow();

    auto minWriteTimestamp = InfinitySystemTimestamp;
    for (auto& record : records) {
        minWriteTimestamp = std::min(minWriteTimestamp, record.WriteTimestamp);
    }

    std::vector<ISource::TMessageBatch> parsedMessages;
    for (i64 recordIndex = 0; recordIndex < std::ssize(records); ++recordIndex) {
        auto& record = records[recordIndex];
        const bool isLastRecord = (recordIndex + 1 == std::ssize(records));
        YT_VERIFY(record.Offset >= State_->PersistedOffsetExclusive);
        YT_VERIFY(record.Offset >= NextReadOffset_,
            Format("Record offset is out of range (Record: %v, NextReadOffset: %v)",
            NYson::ConvertToYsonString(record.Offset, NYson::EYsonFormat::Text),
            NextReadOffset_));
        // Avoid allocating TUnversionedOwningRow for non-last records — use the next record's
        // offset directly, since it equals GetNextOffset of the current record.
        NextReadOffset_ = isLastRecord ? GetNextOffset(record.Offset) : records[recordIndex + 1].Offset;
        if (record.Meta && record.Meta->EventWatermark) {
            ReadEventWatermark_ = std::max(ReadEventWatermark_, record.Meta->EventWatermark);
        }

        State_->OffsetMemory->Register(record.Offset, seqNoProviderResult.UniqueSeqNo);
        auto extractedUniqueSeqNo = State_->OffsetMemory->Extract(record.Offset);
        YT_LOG_FATAL_UNLESS(seqNoProviderResult.UniqueSeqNo >= extractedUniqueSeqNo,
            "Expected that new generated unique seq no is greater than persisted (NewGeneratedUniqueSeqNo: %v, PersistedUniqueSeqNo: %v)",
            seqNoProviderResult.UniqueSeqNo,
            extractedUniqueSeqNo);
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
                        << TErrorAttribute("stream_id", GetContext()->SourceStreamId)
                        << TErrorAttribute("message_id", message.MessageId);
                }
                message.SystemTimestamp = record.WriteTimestamp;
                message.AlignmentTimestamp = alignmentTimestamp;

                if (record.CreateTimestamp == ZeroSystemTimestamp) {
                    THROW_ERROR_EXCEPTION("EventTimestamp is undefined")
                        << TErrorAttribute("stream_id", GetContext()->SourceStreamId)
                        << TErrorAttribute("message_id", message.MessageId);
                }

                message.EventTimestamp = record.CreateTimestamp;
                if (record.Meta) {
                    ApplyFlowQueueMeta(*record.Meta, message, i);
                }

                message.StreamId = GetContext()->SourceStreamId;
                message.PayloadSchema = record.PayloadSchema;
                message.Payload = payload;

                YT_LOG_DEBUG("MessageLifeCycle.Source: message was received (MessageId: %v, StreamId: %v, SystemTimestamp: %v, EventTimestamp: %v, Offset: %v)",
                    message.MessageId,
                    message.StreamId,
                    message.SystemTimestamp,
                    message.EventTimestamp,
                    record.Offset);

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
            });
        }
        TryCollapseOffsetInfo(std::prev(InflightOffsets_.end()));
    }
    if (!empty) {
        State_->LastNotIdleInstant = TInstant::Now();
        State_->LastIdleInstant = TInstant::Zero();
        TryIncreaseMaxOffsetExclusive(NextReadOffset_, false);
    }
    CleanUpInflightOffsets();
    return parsedMessages;
}

////////////////////////////////////////////////////////////////////////////////

TOffset TIntegerOffsetOrderedSourceBase::GetNextOffset(const TOffset& offset) const
{
    i64 intOffset = OffsetToInt(offset);
    return IntToOffset(intOffset + 1);
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
