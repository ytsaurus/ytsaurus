#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>
#include <yt/yt/flow/library/cpp/common/state_provider.h>

#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/flow/lib/serializer/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TJobStateClientContext
    : public TRefCounted
{
    TComputationId ComputationId;
    TJobNamedStateCachePtr StateCache;

    NTables::IKeyStatesPtr KeyStates;
    NTables::IPartitionStatesPtr PartitionStates;

    //! Group-by schema; reported by the key provider as its key schema.
    NTableClient::TTableSchemaPtr KeySchema;

    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
};

DEFINE_REFCOUNTED_TYPE(TJobStateClientContext);

////////////////////////////////////////////////////////////////////////////////

class TRemoteState
    : public TRefCounted
{
public:
    TRemoteState(
        NYsonSerializer::TStatePtr tableState,
        TDynamicStateFormatSpecPtr format);

    NYson::TYsonString Get() const;
    void Set(std::optional<NYson::TYsonString>&& value);
    bool IsEmpty() const;
    void Clear();

    void TrySetFormat(const TDynamicStateFormatSpecPtr& format);

    NYsonSerializer::TStateMutation FlushMutation();

    i64 GetWeight() const;

private:
    const NYsonSerializer::TStatePtr TableState_;
    const NTables::TInternalStatePtr InternalState_;
    TDynamicStateFormatSpecPtr DesiredFormat_;
    bool FormatSynced_;
    bool DoCompress_;
};

using TRemoteStatePtr = TIntrusivePtr<TRemoteState>;

////////////////////////////////////////////////////////////////////////////////

struct TJobStateCacheValue
    : public IStateCacheValue
{
    TJobStateCacheValue(
        TRemoteStatePtr remoteState,
        IStateHolderPtr state)
        : RemoteState(std::move(remoteState))
        , State(std::move(state))
    { }

    void Compress() override
    {
        State = {};
    }

    void Decompress() override
    { }

    i64 GetWeight() override
    {
        return RemoteState->GetWeight();
    }

    TRemoteStatePtr RemoteState;
    IStateHolderPtr State;
};

using TJobStateCacheValuePtr = TIntrusivePtr<TJobStateCacheValue>;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionMutableStateProvider
    : public IMutableStateProvider
{
public:
    TPartitionMutableStateProvider(
        TJobStateClientContextPtr context,
        TPartitionId partitionId,
        std::string name,
        std::function<IStateHolderPtr()> ctor);

    void Reconfigure(TDynamicStateSpecPtr dynamicSpec);
    IStateHolderPtr GetState() override;

    TFuture<void> Init();
    void Sync(IRetryableTransactionPtr transaction);

private:
    const TJobStateClientContextPtr Context_;
    const NLogging::TLogger Logger;
    const TPartitionId PartitionId_;
    const std::string Name_;
    TDynamicStateSpecPtr DynamicSpec_;
    const NTables::IPartitionStatesPtr Table_;
    const std::function<IStateHolderPtr()> Ctor_;

    TRemoteStatePtr RemoteState_;
    IStateHolderPtr State_;
};

using TPartitionMutableStateProviderPtr = TIntrusivePtr<TPartitionMutableStateProvider>;

////////////////////////////////////////////////////////////////////////////////

struct TKeyMutableStateProvider
    : public IMutableStateProvider
{
public:
    TKeyMutableStateProvider(
        TJobStateClientContextPtr context,
        TKey key,
        std::string name,
        std::function<IStateHolderPtr()> ctor);

    void Reconfigure(TDynamicStateSpecPtr dynamicSpec);
    IStateHolderPtr GetState() override;

    TFuture<void> Init();
    void Sync(IRetryableTransactionPtr transaction);

private:
    const TJobStateClientContextPtr Context_;
    const NLogging::TLogger Logger;
    const TKey Key_;
    const std::string Name_;
    TDynamicStateSpecPtr DynamicSpec_;
    const NTables::IKeyStatesPtr Table_;
    const std::function<IStateHolderPtr()> Ctor_;

    TRemoteStatePtr RemoteState_;
    IStateHolderPtr State_;
};

using TKeyMutableStateProviderPtr = TIntrusivePtr<TKeyMutableStateProvider>;

////////////////////////////////////////////////////////////////////////////////

class TJobMutableStateKeyProvider
    : public IMutableStateKeyProvider
{
public:
    TJobMutableStateKeyProvider(
        TJobStateClientContextPtr context,
        std::string name,
        std::function<IStateHolderPtr()> ctor);

    void Reconfigure(TDynamicStateSpecPtr dynamicSpec);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;

    TFuture<void> Init();
    void Sync(IRetryableTransactionPtr transaction);

private:
    const TJobStateClientContextPtr Context_;
    const NLogging::TLogger Logger;
    const std::string Name_;
    TDynamicStateSpecPtr DynamicSpec_;
    const NTables::IKeyStatesPtr Table_;
    const std::function<IStateHolderPtr()> Ctor_;

    THashMap<TKey, IStateHolderPtr> States_;
    THashMap<TKey, TRemoteStatePtr> RemoteStates_;

private:
    IStateHolderPtr GetImpl(const TKey& key, i64 revision);
};

using TJobMutableStateKeyProviderPtr = TIntrusivePtr<TJobMutableStateKeyProvider>;

////////////////////////////////////////////////////////////////////////////////

//! Read-only join over another computation's internal per-key state: reads the shared states
//! table under the target ``computation_id`` and deserializes rows into the user state type,
//! never writing. The per-epoch cache is dropped by ``Reset`` so each epoch reloads the
//! latest committed state.
class TJobJoinedStateKeyProvider
    : public IJoinedStateKeyProvider
{
public:
    TJobJoinedStateKeyProvider(
        TJobStateClientContextPtr context,
        std::string name,
        std::function<IStateHolderPtr()> ctor,
        IPayloadConverterCachePtr converterCache,
        std::optional<THashSet<TStreamId>> keyProviderStreams,
        bool hasKeySchemaOverride,
        TDynamicExpiringJobNamedStateCacheSpecPtr cacheSpec);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;
    const IPayloadConverterCachePtr& GetConverterCache() const override;
    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const override;
    bool HasKeySchemaOverride() const override;

    TFuture<void> Init();
    void Reset();
    void Reconfigure(TDynamicExpiringJobNamedStateCacheSpecPtr cacheSpec);

private:
    const TJobStateClientContextPtr Context_;
    const std::string Name_;
    const NTables::IKeyStatesPtr Table_;
    const std::function<IStateHolderPtr()> Ctor_;
    const IPayloadConverterCachePtr ConverterCache_;
    const std::optional<THashSet<TStreamId>> KeyProviderStreams_;
    const bool HasKeySchemaOverride_;
    const TDynamicStateSpecPtr DynamicSpec_;
    // Null when the job-level state cache is disabled; then every epoch reloads from YT.
    const TExpiringJobNamedStateCachePtr StateCache_;

    THashMap<TKey, IStateHolderPtr> States_;
    THashMap<TKey, TRemoteStatePtr> RemoteStates_;
    THashMap<TKey, TExpiringJobNamedStateCache::TCookie> Cookies_;
};

using TJobJoinedStateKeyProviderPtr = TIntrusivePtr<TJobJoinedStateKeyProvider>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
