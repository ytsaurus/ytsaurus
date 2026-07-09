#include "state_providers.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/key_states.h>
#include <yt/yt/flow/library/cpp/tables/partition_states.h>
#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/flow/lib/serializer/state.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

IYsonSerializable& AsSerializable(const IStateHolderPtr& state)
{
    auto* serializable = dynamic_cast<IYsonSerializable*>(state.Get());
    YT_VERIFY(serializable);
    return *serializable;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRemoteState::TRemoteState(
    NYsonSerializer::TStatePtr tableState,
    TDynamicStateFormatSpecPtr format)
    : TableState_(std::move(tableState))
    , InternalState_(TableState_->GetValueAs<NTables::TInternalState>())
    , DesiredFormat_(std::move(format))
    , FormatSynced_(false)
    , DoCompress_(InternalState_->Compressed.has_value() || (!InternalState_->State.has_value() && DesiredFormat_->Compress))
{ }

NYson::TYsonString TRemoteState::Get() const
{
    if (InternalState_->State && *InternalState_->State) {
        return *InternalState_->State;
    } else if (InternalState_->Compressed && *InternalState_->Compressed) {
        return *InternalState_->Compressed;
    }
    return {};
}

void TRemoteState::Set(std::optional<NYson::TYsonString>&& value)
{
    if (!value || !*value) {
        Clear();
    }

    if (DoCompress_) {
        InternalState_->Compressed = std::move(value);
        InternalState_->State = {};
    } else {
        InternalState_->Compressed = {};
        InternalState_->State = std::move(value);
    }
}

bool TRemoteState::IsEmpty() const
{
    return !InternalState_->State && !InternalState_->Compressed;
}

void TRemoteState::Clear()
{
    InternalState_->State = {};
    InternalState_->Compressed = {};
}

void TRemoteState::TrySetFormat(const TDynamicStateFormatSpecPtr& format)
{
    if (DesiredFormat_.Get() != format.Get()) {
        FormatSynced_ = false;
        DesiredFormat_ = format;
    }
}

NYsonSerializer::TStateMutation TRemoteState::FlushMutation()
{
    if (!FormatSynced_ && (IsEmpty() || RandomNumber<float>() < DesiredFormat_->RecodeProbability)) {
        TableState_->SetFormat(DesiredFormat_);
        FormatSynced_ = true;
        DoCompress_ = DesiredFormat_->Compress;
    }
    if ((DoCompress_ && !InternalState_->Compressed.has_value()) ||
        (!DoCompress_ && !InternalState_->State.has_value()))
    {
        std::swap(InternalState_->Compressed, InternalState_->State);
    }
    TableState_->SetValue(InternalState_);
    return TableState_->FlushMutation();
}

i64 TRemoteState::GetWeight() const
{
    return TableState_->GetSize();
}

////////////////////////////////////////////////////////////////////////////////

TPartitionMutableStateProvider::TPartitionMutableStateProvider(
    TJobStateClientContextPtr context,
    TPartitionId partitionId,
    std::string name,
    std::function<IStateHolderPtr()> ctor)
    : Context_(std::move(context))
    , Logger(Context_->Logger)
    , PartitionId_(partitionId)
    , Name_(std::move(name))
    , DynamicSpec_(New<TDynamicStateSpec>())
    , Table_(Context_->PartitionStates)
    , Ctor_(std::move(ctor))
{
    YT_VERIFY(Table_);
}

void TPartitionMutableStateProvider::Reconfigure(TDynamicStateSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
    Table_->Reconfigure(DynamicSpec_->TableRequest);
}

IStateHolderPtr TPartitionMutableStateProvider::GetState()
{
    if (State_) {
        return State_;
    }
    if (RemoteState_) {
        State_ = Ctor_();
        AsSerializable(State_).Deserialize(RemoteState_->Get());
        return State_;
    }
    THROW_ERROR_EXCEPTION("State is not loaded")
        << TErrorAttribute("name", Name_)
        << TErrorAttribute("partition_id", PartitionId_);
}

TFuture<void> TPartitionMutableStateProvider::Init()
{
    if (Context_->StateCache) {
        if (auto value = DynamicPointerCast<TJobStateCacheValue>(Context_->StateCache->Extract(std::nullopt))) {
            RemoteState_ = value->RemoteState;
            State_ = value->State;
            return OKFuture;
        }
    }
    return BIND([strongThis = MakeStrong(this), this] {
        auto tableState = WaitFor(Table_->Lookup({PartitionId_, Name_})).ValueOrThrow();
        RemoteState_ = New<TRemoteState>(std::move(tableState), DynamicSpec_->Format);
        State_ = nullptr;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TPartitionMutableStateProvider::Sync(IRetryableTransactionPtr transaction)
{
    if (RemoteState_) {
        if (State_) {
            RemoteState_->Set(AsSerializable(State_).Serialize());
        }
        RemoteState_->TrySetFormat(DynamicSpec_->Format);
        auto mutation = RemoteState_->FlushMutation();
        Table_->Write(transaction, {PartitionId_, Name_}, mutation);
        if (Context_->StateCache) {
            Context_->StateCache->Insert(std::nullopt, New<TJobStateCacheValue>(RemoteState_, State_));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TKeyMutableStateProvider::TKeyMutableStateProvider(
    TJobStateClientContextPtr context,
    TKey key,
    std::string name,
    std::function<IStateHolderPtr()> ctor)
    : Context_(std::move(context))
    , Logger(Context_->Logger)
    , Key_(std::move(key))
    , Name_(std::move(name))
    , DynamicSpec_(New<TDynamicStateSpec>())
    , Table_(Context_->KeyStates)
    , Ctor_(std::move(ctor))
{
    YT_VERIFY(Table_);
}

void TKeyMutableStateProvider::Reconfigure(TDynamicStateSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
    Table_->Reconfigure(DynamicSpec_->TableRequest);
}

IStateHolderPtr TKeyMutableStateProvider::GetState()
{
    if (State_) {
        return State_;
    }
    if (RemoteState_) {
        State_ = Ctor_();
        AsSerializable(State_).Deserialize(RemoteState_->Get());
        return State_;
    }
    THROW_ERROR_EXCEPTION("State is not loaded")
        << TErrorAttribute("name", Name_)
        << TErrorAttribute("key", Key_);
}

TFuture<void> TKeyMutableStateProvider::Init()
{
    if (Context_->StateCache) {
        if (auto value = DynamicPointerCast<TJobStateCacheValue>(Context_->StateCache->Extract(Key_))) {
            RemoteState_ = value->RemoteState;
            State_ = value->State;
            return OKFuture;
        }
    }
    return BIND([strongThis = MakeStrong(this), this] {
        auto tableState = WaitFor(Table_->Lookup({Context_->ComputationId, Key_, Name_})).ValueOrThrow();
        RemoteState_ = New<TRemoteState>(std::move(tableState), DynamicSpec_->Format);
        State_ = nullptr;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TKeyMutableStateProvider::Sync(IRetryableTransactionPtr transaction)
{
    if (RemoteState_) {
        if (State_) {
            RemoteState_->Set(AsSerializable(State_).Serialize());
        }
        RemoteState_->TrySetFormat(DynamicSpec_->Format);
        auto mutation = RemoteState_->FlushMutation();
        Table_->Write(transaction, {Context_->ComputationId, Key_, Name_}, mutation);
        if (Context_->StateCache) {
            Context_->StateCache->Insert(Key_, New<TJobStateCacheValue>(RemoteState_, State_));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TJobMutableStateKeyProvider::TJobMutableStateKeyProvider(
    TJobStateClientContextPtr context,
    std::string name,
    std::function<IStateHolderPtr()> ctor)
    : Context_(std::move(context))
    , Logger(Context_->Logger)
    , Name_(std::move(name))
    , DynamicSpec_(New<TDynamicStateSpec>())
    , Table_(Context_->KeyStates)
    , Ctor_(std::move(ctor))
{
    YT_VERIFY(Table_);
}

void TJobMutableStateKeyProvider::Reconfigure(TDynamicStateSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
    Table_->Reconfigure(DynamicSpec_->TableRequest);
}

IStateHolderPtr TJobMutableStateKeyProvider::GetState(const TKey& key)
{
    if (auto iter = States_.find(key); iter != States_.end()) {
        return iter->second;
    }
    if (auto iter = RemoteStates_.find(key); iter != RemoteStates_.end()) {
        auto state = Ctor_();
        AsSerializable(state).Deserialize(iter->second->Get());
        States_[key] = state;
        return state;
    }
    THROW_ERROR_EXCEPTION("State is not loaded")
        << TErrorAttribute("name", Name_)
        << TErrorAttribute("key", key);
}

TFuture<void> TJobMutableStateKeyProvider::PreloadKeyStates(const THashSet<TKey>& keys)
{
    return BIND([strongThis = MakeStrong(this), this, keys = keys] () {
        THashSet<NTables::IKeyStates::TTableKey> toLoad;
        {
            for (const auto& key : keys) {
                if (RemoteStates_.contains(key)) {
                    continue;
                }
                if (Context_->StateCache) {
                    if (auto value = DynamicPointerCast<TJobStateCacheValue>(Context_->StateCache->Extract(key))) {
                        YT_VERIFY(value->RemoteState);
                        RemoteStates_[key] = value->RemoteState;
                        if (value->State) {
                            States_[key] = value->State;
                        }
                        continue;
                    }
                }
                toLoad.insert(NTables::IKeyStates::TTableKey(Context_->ComputationId, key, Name_));
            }
        }
        if (toLoad.empty()) {
            return;
        }
        auto states = WaitFor(Table_->Lookup(std::move(toLoad))).ValueOrThrow();
        {
            for (auto& [tableKey, state] : states) {
                const auto& [computationId, key, name] = tableKey;
                YT_VERIFY(computationId == Context_->ComputationId);
                YT_VERIFY(name == Name_);
                EmplaceOrCrash(RemoteStates_, key, New<TRemoteState>(std::move(state), DynamicSpec_->Format));
            }
        }
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

NTableClient::TTableSchemaPtr TJobMutableStateKeyProvider::GetKeySchema() const
{
    return Context_->KeySchema;
}

TFuture<void> TJobMutableStateKeyProvider::Init()
{
    return OKFuture;
}

void TJobMutableStateKeyProvider::Sync(IRetryableTransactionPtr transaction)
{
    THashMap<NTables::IKeyStates::TTableKey, NYsonSerializer::TStateMutation> mutations;
    for (auto& [key, remoteState] : RemoteStates_) {
        auto state = GetOrDefault(States_, key);
        if (state) {
            remoteState->Set(AsSerializable(state).Serialize());
        }
        remoteState->TrySetFormat(DynamicSpec_->Format);
        mutations[NTables::IKeyStates::TTableKey{Context_->ComputationId, key, Name_}] = remoteState->FlushMutation();
        if (Context_->StateCache) {
            Context_->StateCache->Insert(key, New<TJobStateCacheValue>(remoteState, state));
        }
    }
    RemoteStates_.clear();
    States_.clear();
    Table_->Write(transaction, mutations);
}

////////////////////////////////////////////////////////////////////////////////

TJobJoinedStateKeyProvider::TJobJoinedStateKeyProvider(
    TJobStateClientContextPtr context,
    std::string name,
    std::function<IStateHolderPtr()> ctor,
    IPayloadConverterCachePtr converterCache,
    std::optional<THashSet<TStreamId>> keyProviderStreams,
    bool hasKeySchemaOverride,
    TDynamicExpiringJobNamedStateCacheSpecPtr cacheSpec)
    : Context_(std::move(context))
    , Name_(std::move(name))
    , Table_(Context_->KeyStates)
    , Ctor_(std::move(ctor))
    , ConverterCache_(std::move(converterCache))
    , KeyProviderStreams_(std::move(keyProviderStreams))
    , HasKeySchemaOverride_(hasKeySchemaOverride)
    , DynamicSpec_(New<TDynamicStateSpec>())
    , StateCache_(Context_->StateCache
            ? New<TExpiringJobNamedStateCache>(Context_->StateCache, std::move(cacheSpec))
            : nullptr)
{
    YT_VERIFY(Table_);
}

IStateHolderPtr TJobJoinedStateKeyProvider::GetState(const TKey& key)
{
    if (auto iter = States_.find(key); iter != States_.end()) {
        return iter->second;
    }
    if (auto iter = RemoteStates_.find(key); iter != RemoteStates_.end()) {
        auto state = Ctor_();
        AsSerializable(state).Deserialize(iter->second->Get());
        States_[key] = state;
        return state;
    }
    THROW_ERROR_EXCEPTION("State is not loaded")
        << TErrorAttribute("name", Name_)
        << TErrorAttribute("computation_id", Context_->ComputationId)
        << TErrorAttribute("key", key);
}

TFuture<void> TJobJoinedStateKeyProvider::PreloadKeyStates(const THashSet<TKey>& keys)
{
    return BIND([strongThis = MakeStrong(this), this, keys = keys] () {
        THashSet<NTables::IKeyStates::TTableKey> toLoad;
        for (const auto& key : keys) {
            if (RemoteStates_.contains(key)) {
                continue;
            }
            if (StateCache_) {
                if (auto extracted = StateCache_->Extract(key)) {
                    auto cached = DynamicPointerCast<TJobStateCacheValue>(extracted->first);
                    YT_VERIFY(cached);
                    RemoteStates_[key] = cached->RemoteState;
                    if (cached->State) {
                        States_[key] = cached->State;
                    }
                    Cookies_[key] = extracted->second;
                    continue;
                }
            }
            toLoad.insert(NTables::IKeyStates::TTableKey(Context_->ComputationId, key, Name_));
        }
        if (toLoad.empty()) {
            return;
        }
        auto states = WaitFor(Table_->Lookup(std::move(toLoad))).ValueOrThrow();
        for (auto& [tableKey, state] : states) {
            const auto& [computationId, key, name] = tableKey;
            YT_VERIFY(computationId == Context_->ComputationId);
            YT_VERIFY(name == Name_);
            EmplaceOrCrash(RemoteStates_, key, New<TRemoteState>(std::move(state), DynamicSpec_->Format));
        }
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

NTableClient::TTableSchemaPtr TJobJoinedStateKeyProvider::GetKeySchema() const
{
    return Context_->KeySchema;
}

const IPayloadConverterCachePtr& TJobJoinedStateKeyProvider::GetConverterCache() const
{
    return ConverterCache_;
}

const std::optional<THashSet<TStreamId>>& TJobJoinedStateKeyProvider::GetKeyProviderStreams() const
{
    return KeyProviderStreams_;
}

bool TJobJoinedStateKeyProvider::HasKeySchemaOverride() const
{
    return HasKeySchemaOverride_;
}

TFuture<void> TJobJoinedStateKeyProvider::Init()
{
    return OKFuture;
}

void TJobJoinedStateKeyProvider::Reset()
{
    if (StateCache_) {
        for (const auto& [key, remoteState] : RemoteStates_) {
            auto state = GetOrDefault(States_, key);
            std::optional<TExpiringJobNamedStateCache::TCookie> cookie;
            if (auto it = Cookies_.find(key); it != Cookies_.end()) {
                cookie = it->second;
            }
            StateCache_->Insert(key, New<TJobStateCacheValue>(remoteState, state), cookie);
        }
    }
    RemoteStates_.clear();
    States_.clear();
    Cookies_.clear();
}

void TJobJoinedStateKeyProvider::Reconfigure(TDynamicExpiringJobNamedStateCacheSpecPtr cacheSpec)
{
    if (StateCache_) {
        StateCache_->Reconfigure(std::move(cacheSpec));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
