#pragma once

#include "public.h"

#include "job_init_context.h"
#include "state_providers.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/misc/public.h>
#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/public.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TJobStateManagerContext
    : public TRefCounted
{
    TComputationId ComputationId;
    TPartitionId PartitionId;
    TJobStateCachePtr StateCache; // nullable = cache disabled
    TLoadThroughputThrottlerPtr LoadThroughputThrottler;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
    IInvokerPtr SerializedInvoker;
    NTables::IKeyStatesPtr KeyStates;
    NTables::IPartitionStatesPtr PartitionStates;

    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;

    NTableClient::TTableSchemaPtr KeySchema;
    IPayloadConverterCachePtr ConverterCache;
    THashMap<TResourceId, IResourcePtr> StaticResources;

    THashMap<std::string, TExternalStateManagerSpecPtr> ExternalStateManagers;
    THashMap<std::string, TExternalStateJoinerSpecPtr> ExternalStateJoiners;
    THashMap<std::string, TStateJoinerSpecPtr> StateJoiners;
};

DEFINE_REFCOUNTED_TYPE(TJobStateManagerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicJobStateManagerContext
    : public TRefCounted
{
    TDynamicStateManagerSpecPtr StateManager;

    THashMap<std::string, TDynamicExternalStateManagerSpecPtr> ExternalStateManagers;
    THashMap<std::string, TDynamicExternalStateJoinerSpecPtr> ExternalStateJoiners;
    THashMap<std::string, TDynamicStateJoinerSpecPtr> StateJoiners;
};

DEFINE_REFCOUNTED_TYPE(TDynamicJobStateManagerContext);

////////////////////////////////////////////////////////////////////////////////

// Keeps all created clients internally.
class TJobStateManager
    : public TRefCounted
{
public:
    TJobStateManager(
        TJobStateManagerContextPtr context,
        TDynamicJobStateManagerContextPtr dynamicContext);

    void Reconfigure(TDynamicJobStateManagerContextPtr dynamicContext);

    bool HasPreloadCallbacks() const;
    TFuture<void> PreloadKeyStates(const IInputContextPtr& inputContext);
    void Sync(IRetryableTransactionPtr transaction);

    IJobInitContextPtr CreateContext(std::string prefix = "");

    TFuture<IMutableStateProviderPtr> CreatePartitionMutableStateProvider(std::string path, std::function<IStateHolderPtr()> ctor);
    TFuture<IMutableStateProviderPtr> CreateKeyMutableStateProvider(TKey key, std::string path, std::function<IStateHolderPtr()> ctor);
    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::string path, std::function<IStateHolderPtr()> ctor);
    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::string name, std::function<IStateHolderPtr()> ctor);

    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const;
    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const;

    const THashMap<std::string, IExternalStateManagerPtr>& GetExternalStateManagers() const;
    const THashMap<std::string, IExternalStateJoinerPtr>& GetExternalStateJoiners() const;

    //! Preloads the given visitor-driven joiners with their visit keys, bypassing the
    //! message-driven AutoPreload loop. Each entry maps a joiner name to the keys of the
    //! visits its bound stream delivered this epoch; joiners absent from |joinerKeys| are
    //! not touched.
    TFuture<void> PreloadVisitorDrivenJoiners(const THashMap<std::string, THashSet<TKey>>& joinerKeys);

private:
    const TJobStateManagerContextPtr Context_;
    TDynamicStateManagerSpecPtr DynamicSpec_;

    THashMap<std::string, TWeakPtr<TPartitionMutableStateProvider>> PartitionMutableStateProviders_;
    THashMap<std::pair<TKey, std::string>, TWeakPtr<TKeyMutableStateProvider>> KeyMutableStateProviders_;
    THashMap<std::string, TWeakPtr<TJobMutableStateKeyProvider>> MutableStateKeyProviders_;
    THashMap<std::string, TWeakPtr<TJobJoinedStateKeyProvider>> JoinedStateKeyProviders_;
    THashMap<std::string, TDynamicStateJoinerSpecPtr> DynamicStateJoiners_;

    const THashMap<std::string, IExternalStateManagerPtr> ExternalStateManagers_;
    const THashMap<std::string, IExternalStateJoinerPtr> ExternalStateJoiners_;

private:
    TJobStateClientContextPtr CreateStateClientContext(const std::string& path) const;
    TJobStateClientContextPtr CreateStateJoinerClientContext(const std::string& name, const TStateJoinerSpecPtr& spec) const;
    TDynamicStateSpecPtr GetDynamicSpec(const std::string& path) const;

    static THashMap<std::string, IExternalStateManagerPtr> CreateExternalStateManagers(
        const TJobStateManagerContextPtr& context,
        const TDynamicJobStateManagerContextPtr& dynamicContext);
    static THashMap<std::string, IExternalStateJoinerPtr> CreateExternalStateJoiners(
        const TJobStateManagerContextPtr& context,
        const TDynamicJobStateManagerContextPtr& dynamicContext);

    static TExternalStateManagerContextPtr CreateExternalStateManagerContext(
        const TJobStateManagerContextPtr& context,
        const std::string& name,
        const TExternalStateManagerSpecPtr& spec);
    static TExternalStateJoinerContextPtr CreateExternalStateJoinerContext(
        const TJobStateManagerContextPtr& context,
        const std::string& name,
        const TExternalStateJoinerSpecPtr& spec);
};

DEFINE_REFCOUNTED_TYPE(TJobStateManager);

////////////////////////////////////////////////////////////////////////////////

class TJobInitContext
    : public IJobInitContext
{
public:
    TJobInitContext(
        TJobStateManagerPtr stateManager,
        std::string prefix = "");

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override;
    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override;

    IInitContextPtr AsPartition() const override;
    IInitContextPtr AsKey(TKey key) const override;

    IJobInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const override;
    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const override;

private:
    const TJobStateManagerPtr StateManager_;
    const std::string Prefix_;
};

using TJobInitContextPtr = TIntrusivePtr<TJobInitContext>;

////////////////////////////////////////////////////////////////////////////////

class TKeyInitContext
    : public IInitContext
{
public:
    TKeyInitContext(
        TJobStateManagerPtr stateManager,
        std::string prefix,
        TKey key);

    TFuture<IMutableStateProviderPtr> CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const override;

    IInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

private:
    const TJobStateManagerPtr StateManager_;
    const std::string Prefix_;
    const TKey Key_;
};

using TKeyInitContextPtr = TIntrusivePtr<TKeyInitContext>;

////////////////////////////////////////////////////////////////////////////////

class TPartitionInitContext
    : public IInitContext
{
public:
    TPartitionInitContext(
        TJobStateManagerPtr stateManager,
        std::string prefix);

    TFuture<IMutableStateProviderPtr> CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const override;

    IInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

private:
    const TJobStateManagerPtr StateManager_;
    const std::string Prefix_;
};

using TPartitionInitContextPtr = TIntrusivePtr<TPartitionInitContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
