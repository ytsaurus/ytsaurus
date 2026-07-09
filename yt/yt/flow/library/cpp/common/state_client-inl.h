#pragma once

#ifndef STATE_CLIENT_INL_H_
    #error "Direct inclusion of this file is not allowed, include state_client.h"
    // For the sake of sane code completion.
    #include "state_client.h"
#endif

#include "input_context.h"
#include "message.h"
#include "payload.h"
#include "schema.h"
#include "timer.h"

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/ytree/serialize.h>

#include <util/generic/typetraits.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TConstStateAccessor<T>::TConstStateAccessor(TIntrusivePtr<TStateHolder<T>> state) noexcept
    : State_(std::move(state))
{ }

template <class T>
const T& TConstStateAccessor<T>::operator*() const
{
    return EnsureState()->Get();
}

template <class T>
const T* TConstStateAccessor<T>::operator->() const
{
    return &EnsureState()->Get();
}

template <class T>
const T* TConstStateAccessor<T>::Get() const noexcept
{
    return State_ ? &State_->Get() : nullptr;
}

template <class T>
bool TConstStateAccessor<T>::IsEmpty() const
{
    return EnsureState()->IsEmpty();
}

template <class T>
bool TConstStateAccessor<T>::IsInitialized() const noexcept
{
    return State_ != nullptr;
}

template <class T>
TStateHolder<T>* TConstStateAccessor<T>::EnsureState() const
{
    if (!State_) {
        THROW_ERROR_EXCEPTION("%v is not initialized", TypeName(*this));
    }
    return State_.Get();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T& TStateAccessor<T>::operator*() const
{
    return this->EnsureState()->Get();
}

template <class T>
T* TStateAccessor<T>::operator->() const
{
    return &this->EnsureState()->Get();
}

template <class T>
T* TStateAccessor<T>::Get() const noexcept
{
    return this->State_ ? &this->State_->Get() : nullptr;
}

template <class T>
void TStateAccessor<T>::Clear()
{
    this->EnsureState()->Clear();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMutableStateClient<T>::TMutableStateClient(IMutableStateProviderPtr provider)
    : Provider_(std::move(provider))
{ }

template <class T>
T& TMutableStateClient<T>::operator*() noexcept
{
    return ResolveState()->Get();
}

template <class T>
const T& TMutableStateClient<T>::operator*() const noexcept
{
    return ResolveState()->Get();
}

template <class T>
T* TMutableStateClient<T>::operator->() noexcept
{
    return &ResolveState()->Get();
}

template <class T>
const T* TMutableStateClient<T>::operator->() const noexcept
{
    return &ResolveState()->Get();
}

template <class T>
T* TMutableStateClient<T>::Get() noexcept
{
    return &ResolveState()->Get();
}

template <class T>
const T* TMutableStateClient<T>::Get() const noexcept
{
    return &ResolveState()->Get();
}

template <class T>
void TMutableStateClient<T>::Clear()
{
    ResolveState()->Clear();
}

template <class T>
bool TMutableStateClient<T>::IsEmpty() const
{
    return ResolveState()->IsEmpty();
}

template <class T>
bool TMutableStateClient<T>::IsInitialized() const noexcept
{
    return Provider_ != nullptr;
}

template <class T>
TStateAccessor<T> TMutableStateClient<T>::GetAccessor() const
{
    return TStateAccessor<T>(ResolveState());
}

template <class T>
TIntrusivePtr<TStateHolder<T>> TMutableStateClient<T>::ResolveState() const
{
    // TODO(mikari): resolving (virtual GetState + DynamicPointerCast) on every dereference
    // is expensive on hot paths; cache the resolved holder once the epoch-currency lifecycle
    // is confirmed to allow it.
    NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
    if (!Provider_) {
        THROW_ERROR_EXCEPTION("%v is not initialized", TypeName(*this));
    }
    auto state = DynamicPointerCast<TStateHolder<T>>(Provider_->GetState());
    YT_VERIFY(state);
    return state;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMutableStateKeyClient<T>::TMutableStateKeyClient(IMutableStateKeyProviderPtr provider)
    : Provider_(std::move(provider))
{ }

template <class T>
TStateAccessor<T> TMutableStateKeyClient<T>::GetState(const TKey& key) const
{
    EnsureProvider();
    auto state = DynamicPointerCast<TStateHolder<T>>(Provider_->GetState(key));
    YT_VERIFY(state);
    return TStateAccessor<T>(std::move(state));
}

template <class T>
TStateAccessor<T> TMutableStateKeyClient<T>::GetState(const TInputMessageConstPtr& message) const
{
    return GetState(message->Key);
}

template <class T>
TStateAccessor<T> TMutableStateKeyClient<T>::GetState(const TInputTimerConstPtr& timer) const
{
    return GetState(timer->Key);
}

template <class T>
TFuture<void> TMutableStateKeyClient<T>::PreloadKeyStates(const THashSet<TKey>& keys) const
{
    EnsureProvider();
    return Provider_->PreloadKeyStates(keys);
}

template <class T>
TFuture<void> TMutableStateKeyClient<T>::PreloadKeyStates(const IInputContextPtr& inputContext) const
{
    EnsureProvider();
    return Provider_->PreloadKeyStates(ExtractKeys(inputContext));
}

template <class T>
NTableClient::TTableSchemaPtr TMutableStateKeyClient<T>::GetKeySchema() const
{
    EnsureProvider();
    return Provider_->GetKeySchema();
}

template <class T>
bool TMutableStateKeyClient<T>::IsInitialized() const noexcept
{
    return Provider_ != nullptr;
}

template <class T>
void TMutableStateKeyClient<T>::EnsureProvider() const
{
    NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
    if (!Provider_) {
        THROW_ERROR_EXCEPTION("%v is not initialized", TypeName(*this));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TJoinedStateKeyClient<T>::TJoinedStateKeyClient(IJoinedStateKeyProviderPtr provider)
    : Provider_(std::move(provider))
{ }

template <class T>
TConstStateAccessor<T> TJoinedStateKeyClient<T>::GetState(const TKey& key) const
{
    EnsureProvider();
    auto holder = Provider_->GetState(key);
    if (!holder) {
        return TConstStateAccessor<T>();
    }
    auto state = DynamicPointerCast<TStateHolder<T>>(std::move(holder));
    YT_VERIFY(state);
    return TConstStateAccessor<T>(std::move(state));
}

template <class T>
TConstStateAccessor<T> TJoinedStateKeyClient<T>::GetState(const TInputMessageConstPtr& message) const
{
    EnsureProvider();
    EnsureKeyProviderStream(message->StreamId);
    if (!Provider_->HasKeySchemaOverride()) {
        return GetState(message->Key);
    }
    auto converted = ConvertPayloadToNewSchema(
        message->Payload,
        message->PayloadSchema,
        Provider_->GetKeySchema(),
        Provider_->GetConverterCache());
    return GetState(TKey(converted.Underlying()));
}

template <class T>
TConstStateAccessor<T> TJoinedStateKeyClient<T>::GetState(const TInputTimerConstPtr& timer) const
{
    EnsureProvider();
    EnsureKeyProviderStream(timer->StreamId);
    if (!Provider_->HasKeySchemaOverride()) {
        return GetState(timer->Key);
    }
    auto converted = ConvertPayloadToNewSchema(
        TPayload(timer->Key.Underlying()),
        timer->KeySchema,
        Provider_->GetKeySchema(),
        Provider_->GetConverterCache());
    return GetState(TKey(converted.Underlying()));
}

template <class T>
TFuture<void> TJoinedStateKeyClient<T>::PreloadKeyStates(const THashSet<TKey>& keys) const
{
    EnsureProvider();
    auto schema = Provider_->GetKeySchema();
    for (const auto& key : keys) {
        ValidateKey(key, schema);
    }
    return Provider_->PreloadKeyStates(keys);
}

template <class T>
TFuture<void> TJoinedStateKeyClient<T>::PreloadKeyStates(const IInputContextPtr& inputContext) const
{
    EnsureProvider();
    auto keys = ExtractKeys(
        inputContext,
        Provider_->HasKeySchemaOverride() ? Provider_->GetKeySchema() : nullptr,
        Provider_->GetKeyProviderStreams(),
        Provider_->GetConverterCache());
    return Provider_->PreloadKeyStates(keys);
}

template <class T>
NTableClient::TTableSchemaPtr TJoinedStateKeyClient<T>::GetKeySchema() const
{
    EnsureProvider();
    return Provider_->GetKeySchema();
}

template <class T>
bool TJoinedStateKeyClient<T>::IsInitialized() const noexcept
{
    return Provider_ != nullptr;
}

template <class T>
void TJoinedStateKeyClient<T>::EnsureProvider() const
{
    NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
    if (!Provider_) {
        THROW_ERROR_EXCEPTION("%v is not initialized", TypeName(*this));
    }
}

template <class T>
void TJoinedStateKeyClient<T>::EnsureKeyProviderStream(const TStreamId& streamId) const
{
    const auto& streams = Provider_->GetKeyProviderStreams();
    if (streams && !streams->contains(streamId)) {
        THROW_ERROR_EXCEPTION(
            "Stream %Qv is not declared in joiner's \"key_provider_streams\"",
            streamId);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Serialize(const TMutableStateClient<T>& client, NYson::IYsonConsumer* consumer)
{
    Serialize(*client.Get(), consumer);
}

template <class T>
void Serialize(const TMutableStateClient<T>& client, NYTree::INodePtr node)
{
    Serialize(*client.Get(), node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
