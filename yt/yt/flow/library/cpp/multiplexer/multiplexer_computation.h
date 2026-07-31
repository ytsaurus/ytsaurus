#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Default user state for multiplexer subclasses that don't need to forward
//! anything from the input message into the output rows.
struct TEmptyMultiplexerUserState
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TEmptyMultiplexerUserState);

    static void Register(TRegistrar /*registrar*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Per-key internal state for the multiplexer computation.
//! Tracks the current read offset, circular iteration phase, and collapse bookmark.
struct TMultiplexerKeyState
    : public NYTree::TYsonStruct
{
    //! Whether this key is being actively iterated.
    //! Distinguishes "initialized with defaults" from "no state at all".
    bool IsActive = false;

    //! Current read position. nullopt = beginning (before all data).
    std::optional<TKey> Offset;

    //! Bookmark: position at the time of collapse.
    //! nullopt = no collapse happened.
    std::optional<TKey> InitialStartOffset;

    //! Whether we are in phase 2 of circular iteration:
    //! reading (-inf, InitialStartOffset].
    bool InSecondPhase = false;

    //! Schema describing the layout of `Offset` (and `InitialStartOffset`).
    //! Used by the base class to detect that the subclass's offset format
    //! has changed between pipeline runs and the saved state is no longer valid.
    NTableClient::TTableSchemaPtr OffsetSchema;

    REGISTER_YSON_STRUCT(TMultiplexerKeyState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("is_active", &TThis::IsActive)
            .Default(false);
        registrar.Parameter("offset", &TThis::Offset)
            .Default();
        registrar.Parameter("initial_start_offset", &TThis::InitialStartOffset)
            .Default();
        registrar.Parameter("in_second_phase", &TThis::InSecondPhase)
            .Default(false);
        registrar.Parameter("offset_schema", &TThis::OffsetSchema)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Dynamic parameters for the multiplexer computation.
struct TDynamicMultiplexerParameters
    : public TTransformComputation::TDynamicParameters
{
    //! How often to fire the per-key timer.
    TDuration TimerPeriod;

    //! Preferred batch size passed to DoFetchBatch as limit.
    i64 BatchSize{};

    REGISTER_YSON_STRUCT(TDynamicMultiplexerParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("timer_period", &TThis::TimerPeriod)
            .Default(TDuration::Seconds(5))
            .GreaterThan(TDuration::Seconds(1));
        registrar.Parameter("batch_size", &TThis::BatchSize)
            .Default(1000)
            .GreaterThan(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for multiplexer computations.
//!
//! Multiplexer reads keys from an input stream and for each key iterates
//! over a data source, emitting batches of rows fairly across all active keys.
//! Uses circular iteration to handle collapses (re-read after new events).
//!
//! TUserState must be a TYsonStruct. It is persisted via a separate
//! TMutableStateKeyClient and exposed directly to subclass methods.
//!
//! Offsets are represented as TKey (TUnversionedOwningRow). nullopt means
//! "the very beginning" of the data source. This enables comparison-based
//! validation (monotonicity checks, endOffsetInclusive enforcement).
//!
//! Subclasses must implement DoFetchBatch and DoOnInputMessage.
template <class TUserState>
class TMultiplexerComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicMultiplexerParameters);

    // --- Methods to override ---

    //! Fetch the next batch of data for a key and emit output messages.
    //!
    //! \param key                   The key from group_by_schema.
    //! \param startOffsetExclusive  Position to read after (nullopt = from the very beginning).
    //! \param endOffsetInclusive    If set, read only up to this position (inclusive).
    //!                              The user is responsible for not overshooting this boundary.
    //! \param limit                 Preferred maximum number of rows to fetch.
    //! \param userState             Direct access to user-defined per-key state.
    //! \param output                Output collector for emitting messages.
    //! \returns The next startOffsetExclusive, or nullopt if no more data in the range.
    virtual std::optional<TKey> DoFetchBatch(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit,
        TStateAccessor<TUserState>& userState,
        IOutputCollectorPtr output) = 0;

    //! Called when an input message arrives for a key (both new key and collapse).
    //!
    //! For a new key, userState is empty — initialize it.
    //! For a collapse, userState contains the previous state — update as needed.
    //!
    //! Default implementation does nothing — override only if you need to
    //! forward fields from the input message into the output via userState.
    //!
    //! \param key       The key from group_by_schema.
    //! \param message   The incoming input message.
    //! \param userState Direct access to user-defined per-key state.
    virtual void DoOnInputMessage(
        const TKey& /*key*/,
        const TInputMessageConstPtr& /*message*/,
        TStateAccessor<TUserState>& /*userState*/)
    { }

    //! Returns the schema of offsets currently produced by DoFetchBatch.
    //! Default: nullptr (no validation).
    //!
    //! If non-null, the base class compares it against the schema saved
    //! alongside the per-key offset; on mismatch the offset is dropped and
    //! iteration restarts from scratch for that key.
    virtual NTableClient::TTableSchemaPtr GetCurrentOffsetSchema() const
    {
        return nullptr;
    }

protected:
    void DoInit(IJobInitContextPtr initContext) override
    {
        auto ctx = initContext->WithPrefix("multiplexer");
        ctx->InitClient(InternalStateClient_, "internal");
        ctx->InitClient(UserStateClient_, "user");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) final
    {
        auto state = InternalStateClient_.GetState(message->Key);
        auto userState = UserStateClient_.GetState(message->Key);

        // Unified callback: user initializes (new key) or updates (collapse) their state.
        DoOnInputMessage(message->Key, message, userState);

        if (!state->IsActive) {
            // New key: start iteration from the beginning.
            state->IsActive = true;
            state->Offset = std::nullopt;
            state->InitialStartOffset = std::nullopt;
            state->InSecondPhase = false;
            output->AddTimer(GetNextTimerFiring(message->Key));
        } else {
            // Collapse: bookmark current offset and reset to phase 1.
            state->InitialStartOffset = state->Offset;
            state->InSecondPhase = false;
            // Timer is already scheduled.
        }
    }

    void DoProcessTimer(const TTimer& timer, IOutputCollectorPtr output) final
    {
        auto state = InternalStateClient_.GetState(timer.Key);
        auto userState = UserStateClient_.GetState(timer.Key);
        if (!state->IsActive) {
            state.Clear();
            userState.Clear();
            return;
        }

        // If the subclass's offset format has changed, drop progress and restart.
        auto currentOffsetSchema = GetCurrentOffsetSchema();
        if (currentOffsetSchema &&
            state->OffsetSchema &&
            *currentOffsetSchema != *state->OffsetSchema)
        {
            YT_TLOG_INFO("Offset schema changed, restarting iteration from scratch")
                .With("Key", timer.Key)
                .With("OldSchema", *state->OffsetSchema)
                .With("NewSchema", *currentOffsetSchema);
            state->Offset = std::nullopt;
            state->InitialStartOffset = std::nullopt;
            state->InSecondPhase = false;
        }
        state->OffsetSchema = currentOffsetSchema;

        auto params = GetDynamicParameters();

        // In phase 2, limit reads to InitialStartOffset (inclusive).
        std::optional<TKey> endOffset;
        if (state->InSecondPhase) {
            endOffset = state->InitialStartOffset;
        }

        auto nextOffset = DoFetchBatch(
            timer.Key,
            state->Offset,
            endOffset,
            params->BatchSize,
            userState,
            output);

        if (nextOffset.has_value()) {
            // Validate monotonicity: new offset must be strictly greater than current.
            if (state->Offset.has_value() && !(*nextOffset > *state->Offset)) {
                THROW_ERROR_EXCEPTION("DoFetchBatch returned non-monotonic offset for key %v",
                        timer.Key)
                    << TErrorAttribute("current_offset", state->Offset)
                    << TErrorAttribute("next_offset", *nextOffset);
            }

            // Validate endOffsetInclusive: new offset must not overshoot.
            if (endOffset.has_value() && !(*nextOffset <= *endOffset)) {
                THROW_ERROR_EXCEPTION("DoFetchBatch overshot endOffsetInclusive for key %v",
                        timer.Key)
                    << TErrorAttribute("next_offset", *nextOffset)
                    << TErrorAttribute("end_offset", *endOffset);
            }

            state->Offset = std::move(nextOffset);
            output->AddTimer(GetNextTimerFiring(timer.Key));
        } else {
            // Current phase done.
            if (!state->InSecondPhase && state->InitialStartOffset.has_value()) {
                // Phase 1 complete. Start phase 2: read (-inf, InitialStartOffset].
                state->Offset = std::nullopt;
                state->InSecondPhase = true;
                output->AddTimer(GetNextTimerFiring(timer.Key));
            } else {
                // Iteration complete. Clean up both internal and user state.
                state.Clear();
                userState.Clear();
            }
        }
    }

private:
    TMutableStateKeyClient<TMultiplexerKeyState> InternalStateClient_;
    TMutableStateKeyClient<TUserState> UserStateClient_;

    //! Compute the next timer firing timestamp, distributed evenly across keys
    //! by a hash-based shift within the period.
    TSystemTimestamp GetNextTimerFiring(const TKey& key) const
    {
        ui64 period = GetDynamicParameters()->TimerPeriod.Seconds();
        ui64 shift = THash<TKey>()(key) % period;
        ui64 timestamp = GetCurrentTimestamp().Underlying();
        ui64 nextTimestamp = timestamp - timestamp % period + shift + period;
        return TSystemTimestamp{nextTimestamp};
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
