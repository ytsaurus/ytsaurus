#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Default user state for multiplexer subclasses that don't need to forward
//! anything from the input message into the output rows.
struct TEmptyMultiplexerUserState
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TEmptyMultiplexerUserState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Per-key internal state for the multiplexer process function.
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
    //! Used to detect that the subclass's offset format has changed between pipeline runs.
    NTableClient::TTableSchemaPtr OffsetSchema;

    REGISTER_YSON_STRUCT(TMultiplexerKeyState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicMultiplexerParameters
    : public NYTree::TYsonStruct
{
    //! How often to fire the per-key timer.
    TDuration TimerPeriod;

    //! Preferred batch size passed to FetchBatch as limit.
    i64 BatchSize{};

    REGISTER_YSON_STRUCT(TDynamicMultiplexerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Fairly iterates an external data source for every active input key, persisting progress and
//! user state between timer epochs.
template <class TUserState>
class TMultiplexerProcessFunction
    : public IProcessFunction
{
public:
    explicit TMultiplexerProcessFunction(const TProcessFunctionContextPtr& context);

    void Init(const IRuntimeInitContextPtr& context) final;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) final;

    void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) final;

protected:
    //! Fetches and emits one batch, returning its last offset or null when the range is exhausted.
    virtual std::optional<TKey> FetchBatch(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit,
        TStateAccessor<TUserState>& userState,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) = 0;

    //! Updates the persisted user state for a new activation or collapse.
    virtual void OnInputMessage(
        const TKey& key,
        const TInputMessageConstPtr& message,
        TStateAccessor<TUserState>& userState,
        const IRuntimeContextPtr& context);

    //! Describes persisted offsets; a schema change restarts the key from the beginning.
    virtual NTableClient::TTableSchemaPtr GetCurrentOffsetSchema(
        const IRuntimeContextPtr& context);

private:
    const NLogging::TLogger Logger_;
    TMutableStateKeyClient<TMultiplexerKeyState> InternalStateClient_;
    TMutableStateKeyClient<TUserState> UserStateClient_;

    TSystemTimestamp GetNextTimerFiring(
        const TKey& key,
        const IRuntimeContextPtr& context) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define MULTIPLEXER_PROCESS_FUNCTION_INL_H_
#include "multiplexer_process_function-inl.h"
#undef MULTIPLEXER_PROCESS_FUNCTION_INL_H_
