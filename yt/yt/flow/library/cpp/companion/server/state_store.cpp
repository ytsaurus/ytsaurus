#include "state_store.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

namespace {

IYsonSerializable* AsYsonSerializableOrThrow(
    const IStateHolderPtr& holder,
    const std::string& name)
{
    auto* serializable = dynamic_cast<IYsonSerializable*>(holder.Get());
    THROW_ERROR_EXCEPTION_UNLESS(serializable,
        "Internal state %Qv is not YSON-serializable; "
        "companion internal states must use YSON-serializable holders",
        name);
    return serializable;
}

//! Names arrive in two forms: canonical slash-prefixed prefixes from
//! ExtendStateNamePrefix ("/counter") and verbatim spec names ("counter" or
//! "/state"). Matching is canonical; the wire uses the verbatim declared name.
std::string CanonicalStateName(const std::string& name)
{
    return ExtendStateNamePrefix({}, name);
}

THashMap<std::string, std::string> BuildCanonicalNameMap(const THashSet<std::string>& names)
{
    THashMap<std::string, std::string> result;
    for (const auto& name : names) {
        auto [it, inserted] = result.emplace(CanonicalStateName(name), name);
        // Bad spec input must fail the job, not abort the process.
        THROW_ERROR_EXCEPTION_UNLESS(inserted,
            "State names %Qv and %Qv canonicalize to the same name",
            it->second,
            name);
    }
    return result;
}

void ValidateModifiedKey(
    const TKey& key,
    const THashSet<TKey>& batchKeys,
    const std::string& stateName)
{
    THROW_ERROR_EXCEPTION_UNLESS(batchKeys.contains(key),
        "State %Qv was written for a key outside the current batch",
        stateName);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCompanionInternalStateProvider::TCompanionInternalStateProvider(
    std::string name,
    std::function<IStateHolderPtr()> ctor,
    NTableClient::TTableSchemaPtr keySchema)
    : Name_(std::move(name))
    , Ctor_(std::move(ctor))
    , KeySchema_(std::move(keySchema))
{
    // Fail fast if the state type cannot round-trip through the wire.
    AsYsonSerializableOrThrow(Ctor_(), Name_);
}

IStateHolderPtr TCompanionInternalStateProvider::GetState(const TKey& key)
{
    auto it = Holders_.find(key);
    if (it == Holders_.end()) {
        auto holder = Ctor_();
        if (auto incomingIt = Incoming_.find(key); incomingIt != Incoming_.end()) {
            AsYsonSerializableOrThrow(holder, Name_)->Deserialize(NYson::TYsonString(TString(incomingIt->second)));
        }
        it = Holders_.emplace(key, std::move(holder)).first;
    }
    return it->second;
}

TFuture<void> TCompanionInternalStateProvider::PreloadKeyStates(const THashSet<TKey>& /*keys*/)
{
    // The whole batch state is already in memory.
    return OKFuture;
}

NTableClient::TTableSchemaPtr TCompanionInternalStateProvider::GetKeySchema() const
{
    return KeySchema_;
}

void TCompanionInternalStateProvider::LoadBatch(
    const NCompanion::TStateHolder<std::string>* incoming)
{
    Holders_.clear();
    Incoming_.clear();
    if (!incoming) {
        return;
    }
    for (const auto& item : incoming->StateItems) {
        if (!item.Reset) {
            Incoming_[item.Key] = item.State;
        }
    }
}

void TCompanionInternalStateProvider::CollectModified(
    std::vector<NCompanion::TStateItem<std::string>>* items,
    const THashSet<TKey>& batchKeys) const
{
    for (const auto& [key, holder] : Holders_) {
        auto serialized = dynamic_cast<IYsonSerializable&>(*holder).Serialize();
        auto incomingIt = Incoming_.find(key);
        if (serialized) {
            auto bytes = std::string(serialized->ToString());
            if (incomingIt != Incoming_.end() && incomingIt->second == bytes) {
                continue;
            }
            ValidateModifiedKey(key, batchKeys, Name_);
            items->push_back({.Key = key, .Reset = false, .State = std::move(bytes)});
        } else if (incomingIt != Incoming_.end()) {
            ValidateModifiedKey(key, batchKeys, Name_);
            items->push_back({.Key = key, .Reset = true, .State = {}});
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TCompanionExternalStateManager::TCompanionExternalStateManager(
    std::string name,
    NTableClient::TTableSchemaPtr keySchema)
    : Name_(std::move(name))
    , KeySchema_(std::move(keySchema))
{ }

IStateHolderPtr TCompanionExternalStateManager::GetState(const TKey& key)
{
    auto it = Holders_.find(key);
    if (it == Holders_.end()) {
        THROW_ERROR_EXCEPTION_UNLESS(StateSchema_,
            "External state %Qv schema is unknown: the batch carried no items of this state",
            Name_);
        auto holder = New<TStateHolder<TSimpleExternalState>>();
        holder->Get().Schema = StateSchema_;
        if (auto incomingIt = Incoming_.find(key); incomingIt != Incoming_.end()) {
            holder->Get().Payload = incomingIt->second;
        } else {
            holder->Get().Payload = TPayloadBuilder(StateSchema_).Finish();
        }
        it = Holders_.emplace(key, std::move(holder)).first;
    }
    return it->second;
}

TFuture<void> TCompanionExternalStateManager::PreloadKeyStates(const THashSet<TKey>& /*keys*/)
{
    return OKFuture;
}

NTableClient::TTableSchemaPtr TCompanionExternalStateManager::GetKeySchema() const
{
    return KeySchema_;
}

void TCompanionExternalStateManager::Sync(IRetryableTransactionPtr /*transaction*/)
{ }

void TCompanionExternalStateManager::ValidateStateClass(const std::type_info& expectedStateType) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        expectedStateType == typeid(TSimpleExternalState),
        "Companion external state manager only supports TSimpleExternalState");
}

IExternalStateManager::TParametersPtr TCompanionExternalStateManager::GetParametersBase() const
{
    return New<TParameters>();
}

IExternalStateManager::TDynamicParametersPtr TCompanionExternalStateManager::GetDynamicParametersBase() const
{
    return New<TDynamicParameters>();
}

void TCompanionExternalStateManager::LoadBatch(
    const NCompanion::TStateHolder<TPayload>* incoming)
{
    Holders_.clear();
    Incoming_.clear();
    if (!incoming) {
        return;
    }
    if (incoming->Schema) {
        StateSchema_ = incoming->Schema;
    }
    for (const auto& item : incoming->StateItems) {
        if (!item.Reset) {
            Incoming_[item.Key] = item.State;
        }
    }
}

void TCompanionExternalStateManager::CollectModified(
    NCompanion::TStateHolder<TPayload>* holder,
    const THashSet<TKey>& batchKeys) const
{
    holder->StateName = Name_;
    holder->Schema = StateSchema_;
    for (const auto& [key, stateHolder] : Holders_) {
        const auto& state = stateHolder->Get();
        auto incomingIt = Incoming_.find(key);
        if (incomingIt != Incoming_.end() && incomingIt->second == state.Payload) {
            continue;
        }
        if (state.IsEmpty()) {
            if (incomingIt == Incoming_.end()) {
                continue;
            }
            ValidateModifiedKey(key, batchKeys, Name_);
            holder->StateItems.push_back({.Key = key, .Reset = true, .State = {}});
        } else {
            ValidateModifiedKey(key, batchKeys, Name_);
            holder->StateItems.push_back({.Key = key, .Reset = false, .State = state.Payload});
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// Expression columns are dropped because evaluating them would pull the query engine into every
// companion binary; these keys only index Holders_, where the plain columns already separate them.
TCompanionExternalStateJoiner::TCompanionExternalStateJoiner(
    std::string name,
    TCompanionExternalStateJoinerConfig config)
    : Name_(std::move(name))
    , WireKeySchema_(std::move(config.KeySchema))
    , KeySchema_(config.HasKeySchemaOverride && WireKeySchema_
            ? StripExpressionColumns(WireKeySchema_)
            : WireKeySchema_)
    , ConverterCache_(std::move(config.ConverterCache))
    , KeyProviderStreams_(std::move(config.KeyProviderStreams))
    , HasKeySchemaOverride_(config.HasKeySchemaOverride)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        !HasKeySchemaOverride_ || (KeySchema_ && ConverterCache_),
        "External state joiner %Qv with key schema override requires a key schema and converter cache",
        Name_);
}

IStateHolderPtr TCompanionExternalStateJoiner::GetState(const TKey& key)
{
    auto it = Holders_.find(key);
    // A key the batch carried no joined state for surfaces as an uninitialized accessor.
    return it != Holders_.end() ? it->second : nullptr;
}

TFuture<void> TCompanionExternalStateJoiner::PreloadKeyStates(const THashSet<TKey>& /*keys*/)
{
    return OKFuture;
}

NTableClient::TTableSchemaPtr TCompanionExternalStateJoiner::GetKeySchema() const
{
    return KeySchema_;
}

const IPayloadConverterCachePtr& TCompanionExternalStateJoiner::GetConverterCache() const
{
    return ConverterCache_;
}

const std::optional<THashSet<TStreamId>>& TCompanionExternalStateJoiner::GetKeyProviderStreams() const
{
    return KeyProviderStreams_;
}

bool TCompanionExternalStateJoiner::HasKeySchemaOverride() const
{
    return HasKeySchemaOverride_;
}

void TCompanionExternalStateJoiner::Reset()
{ }

void TCompanionExternalStateJoiner::ValidateStateClass(const std::type_info& expectedStateType) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        expectedStateType == typeid(TSimpleExternalState),
        "Companion external state joiner only supports TSimpleExternalState");
}

IExternalStateJoiner::TParametersPtr TCompanionExternalStateJoiner::GetParametersBase() const
{
    return New<TParameters>();
}

IExternalStateJoiner::TDynamicParametersPtr TCompanionExternalStateJoiner::GetDynamicParametersBase() const
{
    return New<TDynamicParameters>();
}

void TCompanionExternalStateJoiner::LoadBatch(
    const NCompanion::TStateHolder<TPayload>* incoming)
{
    Holders_.clear();
    if (!incoming) {
        return;
    }
    // Index each state under both layouts: the key the worker sent, and the stripped key
    // GetKeySchema() advertises. Stripping cannot merge distinct keys, so the two entries of one
    // state never collide with another's. The converter depends on the schema pair only.
    IPayloadConverterPtr keyConverter;
    if (KeySchema_ != WireKeySchema_) {
        keyConverter = ConverterCache_->Get(WireKeySchema_, KeySchema_);
    }
    for (const auto& item : incoming->StateItems) {
        if (item.Reset) {
            continue;
        }
        auto holder = New<TStateHolder<TSimpleExternalState>>();
        holder->Get().Schema = incoming->Schema;
        holder->Get().Payload = item.State;
        // The converter addresses the row by the wire schema's column positions, so only re-lay out
        // a key that is actually laid out on it.
        if (keyConverter && item.Key.Underlying().GetCount() == WireKeySchema_->GetColumnCount()) {
            Holders_[keyConverter->Convert(item.Key)] = holder;
        }
        Holders_[item.Key] = std::move(holder);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCompanionStateStore::TCompanionStateStore(
    THashSet<std::string> internalStateNames,
    THashSet<std::string> externalStateNames,
    THashSet<std::string> joinedStateNames,
    NTableClient::TTableSchemaPtr keySchema,
    THashMap<std::string, TCompanionExternalStateJoinerConfig> joinedStateConfigs)
    : InternalStateNames_(BuildCanonicalNameMap(internalStateNames))
    , ExternalStateNames_(BuildCanonicalNameMap(externalStateNames))
    , JoinedStateNames_(BuildCanonicalNameMap(joinedStateNames))
    , KeySchema_(std::move(keySchema))
    , JoinedStateConfigs_(std::move(joinedStateConfigs))
{ }

IMutableStateKeyProviderPtr TCompanionStateStore::RegisterInternalState(
    const std::string& name,
    std::function<IStateHolderPtr()> ctor)
{
    auto declaredIt = InternalStateNames_.find(CanonicalStateName(name));
    THROW_ERROR_EXCEPTION_IF(declaredIt == InternalStateNames_.end(),
        "Internal state %Qv is not declared in the computation's \"internal_states\" parameter",
        name);
    const auto& stateName = declaredIt->second;
    auto it = InternalStates_.find(stateName);
    if (it == InternalStates_.end()) {
        it = InternalStates_.emplace(
            stateName,
            New<TCompanionInternalStateProvider>(stateName, std::move(ctor), KeySchema_))
            .first;
    }
    return it->second;
}

IExternalStateManagerPtr TCompanionStateStore::GetExternalStateManager(const std::string& name)
{
    auto declaredIt = ExternalStateNames_.find(CanonicalStateName(name));
    THROW_ERROR_EXCEPTION_IF(declaredIt == ExternalStateNames_.end(),
        "External state %Qv is not declared in the computation's \"external_state_managers\"",
        name);
    const auto& stateName = declaredIt->second;
    auto it = ExternalStates_.find(stateName);
    if (it == ExternalStates_.end()) {
        it = ExternalStates_.emplace(
            stateName,
            New<TCompanionExternalStateManager>(stateName, KeySchema_))
            .first;
    }
    return it->second;
}

IExternalStateJoinerPtr TCompanionStateStore::GetExternalStateJoiner(const std::string& name)
{
    auto declaredIt = JoinedStateNames_.find(CanonicalStateName(name));
    THROW_ERROR_EXCEPTION_IF(declaredIt == JoinedStateNames_.end(),
        "External state joiner %Qv is not declared in the computation's \"external_state_joiners\"",
        name);
    const auto& stateName = declaredIt->second;
    auto it = JoinedStates_.find(stateName);
    if (it == JoinedStates_.end()) {
        TCompanionExternalStateJoinerConfig config;
        if (auto configIt = JoinedStateConfigs_.find(stateName); configIt != JoinedStateConfigs_.end()) {
            config = configIt->second;
        } else {
            config.KeySchema = KeySchema_;
        }
        it = JoinedStates_.emplace(
            stateName,
            New<TCompanionExternalStateJoiner>(stateName, std::move(config)))
            .first;
    }
    return it->second;
}

void TCompanionStateStore::LoadBatch(const TBatchInput& input)
{
    BatchKeys_.clear();
    for (const auto& message : input.Messages) {
        BatchKeys_.insert(message->Key);
    }
    for (const auto& timer : input.Timers) {
        BatchKeys_.insert(timer->Key);
    }
    for (const auto& visit : input.Visits) {
        BatchKeys_.insert(visit->Key);
    }

    for (const auto& [name, provider] : InternalStates_) {
        auto it = input.InternalStates.find(name);
        provider->LoadBatch(it != input.InternalStates.end() ? &it->second : nullptr);
    }
    for (const auto& [name, manager] : ExternalStates_) {
        auto it = input.ExternalStates.find(name);
        manager->LoadBatch(it != input.ExternalStates.end() ? &it->second : nullptr);
    }
    for (const auto& [name, joiner] : JoinedStates_) {
        auto it = input.JoinedExternalStates.find(name);
        joiner->LoadBatch(it != input.JoinedExternalStates.end() ? &it->second : nullptr);
    }
}

void TCompanionStateStore::CollectModified(
    std::vector<NCompanion::TStateHolder<std::string>>* internalStates,
    std::vector<NCompanion::TStateHolder<TPayload>>* externalStates) const
{
    for (const auto& [name, provider] : InternalStates_) {
        NCompanion::TStateHolder<std::string> holder;
        holder.StateName = name;
        provider->CollectModified(&holder.StateItems, BatchKeys_);
        if (!holder.StateItems.empty()) {
            internalStates->push_back(std::move(holder));
        }
    }
    for (const auto& [name, manager] : ExternalStates_) {
        NCompanion::TStateHolder<TPayload> holder;
        manager->CollectModified(&holder, BatchKeys_);
        if (!holder.StateItems.empty()) {
            externalStates->push_back(std::move(holder));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
