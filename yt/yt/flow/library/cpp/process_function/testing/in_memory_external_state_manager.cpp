#include "in_memory_external_state_manager.h"

#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

TInMemorySimpleExternalStateManager::TInMemorySimpleExternalStateManager(
    NTableClient::TTableSchemaPtr stateSchema,
    NTableClient::TTableSchemaPtr keySchema)
    : StateSchema_(std::move(stateSchema))
    , KeySchema_(std::move(keySchema))
{ }

IStateHolderPtr TInMemorySimpleExternalStateManager::GetState(const TKey& key)
{
    auto it = States_.find(key);
    if (it == States_.end()) {
        auto holder = New<TStateHolder<TSimpleExternalState>>();
        holder->Get().Schema = StateSchema_;
        holder->Get().Payload = TPayloadBuilder(StateSchema_).Finish();
        it = States_.emplace(key, std::move(holder)).first;
    }
    return it->second;
}

TFuture<void> TInMemorySimpleExternalStateManager::PreloadKeyStates(const THashSet<TKey>& /*keys*/)
{
    // States are created lazily on GetState, so there is nothing to preload.
    return OKFuture;
}

NTableClient::TTableSchemaPtr TInMemorySimpleExternalStateManager::GetKeySchema() const
{
    return KeySchema_;
}

void TInMemorySimpleExternalStateManager::Sync(IRetryableTransactionPtr /*transaction*/)
{ }

void TInMemorySimpleExternalStateManager::ValidateStateClass(const std::type_info& expectedStateType) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        expectedStateType == typeid(TSimpleExternalState),
        "In-memory external state manager only supports TSimpleExternalState");
}

IExternalStateManager::TParametersPtr TInMemorySimpleExternalStateManager::GetParametersBase() const
{
    return New<TParameters>();
}

IExternalStateManager::TDynamicParametersPtr TInMemorySimpleExternalStateManager::GetDynamicParametersBase() const
{
    return New<TDynamicParameters>();
}

////////////////////////////////////////////////////////////////////////////////

TInMemorySimpleExternalStateJoiner::TInMemorySimpleExternalStateJoiner(
    NTableClient::TTableSchemaPtr stateSchema,
    NTableClient::TTableSchemaPtr keySchema)
    : StateSchema_(std::move(stateSchema))
    , KeySchema_(std::move(keySchema))
{ }

TIntrusivePtr<TStateHolder<TSimpleExternalState>> TInMemorySimpleExternalStateJoiner::GetMutableState(const TKey& key)
{
    auto it = States_.find(key);
    if (it == States_.end()) {
        auto holder = New<TStateHolder<TSimpleExternalState>>();
        holder->Get().Schema = StateSchema_;
        holder->Get().Payload = TPayloadBuilder(StateSchema_).Finish();
        it = States_.emplace(key, std::move(holder)).first;
    }
    return it->second;
}

IStateHolderPtr TInMemorySimpleExternalStateJoiner::GetState(const TKey& key)
{
    return GetMutableState(key);
}

TFuture<void> TInMemorySimpleExternalStateJoiner::PreloadKeyStates(const THashSet<TKey>& /*keys*/)
{
    // States are created lazily on GetState, so there is nothing to preload.
    return OKFuture;
}

NTableClient::TTableSchemaPtr TInMemorySimpleExternalStateJoiner::GetKeySchema() const
{
    return KeySchema_;
}

const IPayloadConverterCachePtr& TInMemorySimpleExternalStateJoiner::GetConverterCache() const
{
    return ConverterCache_;
}

const std::optional<THashSet<TStreamId>>& TInMemorySimpleExternalStateJoiner::GetKeyProviderStreams() const
{
    return KeyProviderStreams_;
}

bool TInMemorySimpleExternalStateJoiner::HasKeySchemaOverride() const
{
    return false;
}

void TInMemorySimpleExternalStateJoiner::Reset()
{ }

void TInMemorySimpleExternalStateJoiner::ValidateStateClass(const std::type_info& expectedStateType) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        expectedStateType == typeid(TSimpleExternalState),
        "In-memory external state joiner only supports TSimpleExternalState");
}

IExternalStateJoiner::TParametersPtr TInMemorySimpleExternalStateJoiner::GetParametersBase() const
{
    return New<TParameters>();
}

IExternalStateJoiner::TDynamicParametersPtr TInMemorySimpleExternalStateJoiner::GetDynamicParametersBase() const
{
    return New<TDynamicParameters>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
