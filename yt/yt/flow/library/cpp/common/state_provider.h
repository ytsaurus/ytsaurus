#pragma once

#include "key.h"
#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <util/generic/hash_set.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Type-erased backend yielding the current #IStateHolder for a single (partition or
//! single-key) mutable state. Implemented by the worker/controller storage and
//! substituted by in-memory mocks; the typed #TMutableStateClient casts the
//! result to #TStateHolder.
struct IMutableStateProvider
    : public TRefCounted
{
    virtual IStateHolderPtr GetState() = 0;
};

DEFINE_REFCOUNTED_TYPE(IMutableStateProvider)

////////////////////////////////////////////////////////////////////////////////

//! Type-erased backend yielding the current #IStateHolder per key for a mutable
//! key-indexed state. Implemented both by the internal job-side store and by
//! #IExternalStateManager; consumed by #TMutableStateKeyClient.
struct IMutableStateKeyProvider
    : public TRefCounted
{
    //! Throws for a key that was not preloaded in the current epoch.
    virtual IStateHolderPtr GetState(const TKey& key) = 0;
    //! Loads |keys| for the current epoch; incremental: only keys not loaded yet are fetched,
    //! already-loaded states are left untouched. Calls are awaited one at a time.
    virtual TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) = 0;
    //! Stages the deletion of |key| at the next Sync without loading it. Terminal for the epoch:
    //! #GetState() throws and #PreloadKeyStates() skips the key afterwards. The default throws;
    //! a store that can delete by key alone overrides it.
    virtual void EraseKeyState(const TKey& key);
    //! Returns null when the underlying store has no explicit key schema.
    virtual NTableClient::TTableSchemaPtr GetKeySchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMutableStateKeyProvider)

////////////////////////////////////////////////////////////////////////////////

//! Type-erased read-only backend yielding the current #IStateHolder per key for a
//! joined state, plus the primitives #TJoinedStateKeyClient needs to derive a
//! key from a message/timer. Implemented by #IExternalStateJoiner.
struct IJoinedStateKeyProvider
    : public TRefCounted
{
    virtual IStateHolderPtr GetState(const TKey& key) = 0;
    virtual TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) = 0;
    virtual NTableClient::TTableSchemaPtr GetKeySchema() const = 0;
    virtual const IPayloadConverterCachePtr& GetConverterCache() const = 0;
    //! ``nullopt`` means "any input stream".
    virtual const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const = 0;
    virtual bool HasKeySchemaOverride() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinedStateKeyProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
