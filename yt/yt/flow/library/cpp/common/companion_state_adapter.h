#pragma once

#include "key.h"
#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The wire format of a companion-visible external state payload.
//! Numeric values are part of the companion protocol; slots 2 (opaque) and
//! 3 (raw row) are reserved for future formats and must not be reused.
DEFINE_ENUM(EStateFormat,
    ((SimpleRow) (0))
    ((Proto) (1))
);

////////////////////////////////////////////////////////////////////////////////

//! Description of one companion-visible external state. #StateName and
//! #Format are fixed for the adapter's lifetime; #Schema may only become
//! known once states are loaded, so consult #Describe at the point of use.
struct TCompanionStateDescriptor
{
    std::string StateName;
    EStateFormat Format = EStateFormat::SimpleRow;
    //! The state row schema; set for #EStateFormat::SimpleRow.
    NTableClient::TTableSchemaPtr Schema;
    //! Fully qualified proto message name; set for #EStateFormat::Proto.
    std::string ProtoType;
};

////////////////////////////////////////////////////////////////////////////////

//! Protocol-agnostic bridge between an external state manager (or joiner)
//! and a companion computation: encodes preloaded states into wire payloads
//! and applies companion-returned payloads back.
//!
//! All keys passed to the adapter must have been preloaded into the owning
//! manager/joiner for the current epoch; the adapter does not load states.
struct ICompanionStateAdapter
    : public TRefCounted
{
    virtual TCompanionStateDescriptor Describe() const = 0;

    //! Encodes the state for |key| into its wire payload.
    //! A null ref means the key currently has no state to send.
    virtual TSharedRef EncodeState(const TKey& key) = 0;

    //! Applies a companion-returned payload to the state for |key|.
    //! Throws for read-only (joined) states.
    virtual void ApplyState(const TKey& key, TSharedRef payload) = 0;

    //! Clears the state for |key|.
    //! Throws for read-only (joined) states.
    virtual void ResetState(const TKey& key) = 0;

    //! Extracts the state keys this adapter should encode for |input|.
    //! Joined states may be keyed by a schema of their own, in which case
    //! keys are re-extracted from the payloads under it (matching what the
    //! owning joiner preloaded); the default is the input's own keys.
    virtual THashSet<TKey> ExtractKeys(const IInputContextPtr& input) const;
};

DEFINE_REFCOUNTED_TYPE(ICompanionStateAdapter);

////////////////////////////////////////////////////////////////////////////////

//! Key extraction for read-only joined states, shared by joiner adapters:
//! reproduces TJoinedStateKeyClient::ExtractKeys().
THashSet<TKey> ExtractJoinedStateKeys(
    const IJoinedStateKeyProvider& provider,
    const IInputContextPtr& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
