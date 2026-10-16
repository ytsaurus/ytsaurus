#pragma once

#include "companion_model.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Direction of a state holder on the wire, driving the codec's validation:
//! request states (worker -> companion) carry no resets and may hold empty
//! payloads, and their schema is decoded — the companion builds payloads
//! against it; response states (companion -> worker) must carry a payload
//! unless reset, and their schema is not parsed — the worker never consumed
//! it, and staying lenient keeps foreign companions' schema bytes inert.
enum class EStateDirection
{
    Request,
    Response,
};

//! Wire codec for state holders, shared by the worker-side client and the C++
//! companion server so both ends of the protocol evolve in one place.
template <typename TStatePayload, typename TProtoState>
void SerializeStateHolder(
    TProtoState* protoState,
    const TStateHolder<TStatePayload>& state,
    EStateDirection direction);

template <typename TStatePayload, typename TProtoState>
TStateHolder<TStatePayload> ParseStateHolder(
    const TProtoState& protoState,
    EStateDirection direction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion

#define STATE_CODEC_INL_H_
#include "state_codec-inl.h"
#undef STATE_CODEC_INL_H_
