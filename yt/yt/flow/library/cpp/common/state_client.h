#pragma once

#include "key.h"
#include "public.h"
#include "state_provider.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/public.h>

#include <util/generic/noncopyable.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TStateHolder;

////////////////////////////////////////////////////////////////////////////////

//! Read-only, move-only, epoch-scoped view of a state. Returned by
//! #TJoinedStateKeyClient::GetState. Must not be stored past the current epoch:
//! the referenced state may be evicted at the epoch boundary.
template <class T>
class TConstStateAccessor
    : public TMoveOnly
{
public:
    TConstStateAccessor() = default;
    explicit TConstStateAccessor(TIntrusivePtr<TStateHolder<T>> state) noexcept;

    //! Dereferencing an uninitialized accessor throws via #EnsureState. An unreadable joiner key
    //! surfaces as such an accessor.
    const T& operator*() const;
    const T* operator->() const;
    const T* Get() const noexcept;

    bool IsEmpty() const;
    bool IsInitialized() const noexcept;

protected:
    TIntrusivePtr<TStateHolder<T>> State_;

    TStateHolder<T>* EnsureState() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Mutable counterpart of #TConstStateAccessor. Returned by #TMutableStateKeyClient
//! and #TMutableStateClient::GetAccessor. A #TStateAccessor is-a #TConstStateAccessor,
//! so it can be passed wherever a read-only accessor is expected.
template <class T>
class TStateAccessor
    : public TConstStateAccessor<T>
{
public:
    using TConstStateAccessor<T>::TConstStateAccessor;

    //! Not noexcept: see #TConstStateAccessor::operator*.
    T& operator*() const;
    T* operator->() const;
    T* Get() const noexcept;

    void Clear();
};

////////////////////////////////////////////////////////////////////////////////

//! Persistent typed handle over an R/W state. Survives epoch boundaries — always
//! resolves (via #IMutableStateProvider) to the current live state.
template <class T>
class TMutableStateClient
{
public:
    TMutableStateClient() = default;
    explicit TMutableStateClient(IMutableStateProviderPtr provider);

    T& operator*() noexcept;
    const T& operator*() const noexcept;
    T* operator->() noexcept;
    const T* operator->() const noexcept;

    T* Get() noexcept;
    const T* Get() const noexcept;

    void Clear();
    bool IsEmpty() const;
    bool IsInitialized() const noexcept;

    //! Epoch-scoped view over the same state, suitable for passing to helpers.
    TStateAccessor<T> GetAccessor() const;

private:
    IMutableStateProviderPtr Provider_;

    TIntrusivePtr<TStateHolder<T>> ResolveState() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Persistent typed key-indexed handle over an R/W state. Backed (via
//! #IMutableStateKeyProvider) by either the internal job-side store or an
//! external state manager. #GetState() hands out an epoch-scoped #TStateAccessor.
template <class T>
class TMutableStateKeyClient
{
public:
    TMutableStateKeyClient() = default;
    explicit TMutableStateKeyClient(IMutableStateKeyProviderPtr provider);

    TStateAccessor<T> GetState(const TKey& key) const;
    TStateAccessor<T> GetState(const TInputMessageConstPtr& message) const;
    TStateAccessor<T> GetState(const TInputTimerConstPtr& timer) const;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) const;
    TFuture<void> PreloadKeyStates(const IInputContextPtr& inputContext) const;
    NTableClient::TTableSchemaPtr GetKeySchema() const;
    bool IsInitialized() const noexcept;

private:
    IMutableStateKeyProviderPtr Provider_;

    void EnsureProvider() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Persistent typed key-indexed handle over an R/O state. Backed (via
//! #IJoinedStateKeyProvider) by an external state joiner; resolves a key from a
//! message/timer here in the client. #GetState() hands out a #TConstStateAccessor.
template <class T>
class TJoinedStateKeyClient
{
public:
    TJoinedStateKeyClient() = default;
    explicit TJoinedStateKeyClient(IJoinedStateKeyProviderPtr provider);

    TConstStateAccessor<T> GetState(const TKey& key) const;
    TConstStateAccessor<T> GetState(const TInputMessageConstPtr& message) const;
    TConstStateAccessor<T> GetState(const TInputTimerConstPtr& timer) const;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) const;
    TFuture<void> PreloadKeyStates(const IInputContextPtr& inputContext) const;
    NTableClient::TTableSchemaPtr GetKeySchema() const;
    bool IsInitialized() const noexcept;

private:
    IJoinedStateKeyProviderPtr Provider_;

    void EnsureProvider() const;
    void EnsureKeyProviderStream(const TStreamId& streamId) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Serialize(const TMutableStateClient<T>& client, NYson::IYsonConsumer* consumer);

template <class T>
void Serialize(const TMutableStateClient<T>& client, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define STATE_CLIENT_INL_H_
#include "state_client-inl.h"
#undef STATE_CLIENT_INL_H_
