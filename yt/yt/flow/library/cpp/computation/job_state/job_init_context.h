#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IJobInitContext
    : public TRefCounted
{
public:
    template <class TStateHolder>
    TFuture<TMutableStateKeyClient<TStateHolder>> CreateMutableStateKeyClient(TStringBuf name) const
    {
        return WithPrefix(name)->CreateMutableStateKeyClient<TStateHolder>();
    }

    template <class TStateHolder>
    TFuture<TMutableStateKeyClient<TStateHolder>> CreateMutableStateKeyClient() const
    {
        return CreateMutableStateKeyProvider(&New<TYsonSerializableStateHolder<TStateHolder>>)
            .AsUnique()
            .Apply(BIND([] (TErrorOr<IMutableStateKeyProviderPtr>&& result) {
                return TMutableStateKeyClient<TStateHolder>(std::move(result.ValueOrThrow()));
            }));
    }

    template <class TStateHolder>
    void InitClient(TMutableStateKeyClient<TStateHolder>& client) const
    {
        client = NConcurrency::WaitFor(CreateMutableStateKeyClient<TStateHolder>()).ValueOrThrow();
    }

    template <class TStateHolder>
    void InitClient(TMutableStateKeyClient<TStateHolder>& client, TStringBuf name) const
    {
        WithPrefix(name)->InitClient<TStateHolder>(client);
    }

    //! Read-only join over another computation's internal state, declared in
    //! ``TComputationSpec::StateJoiners[GetPrefix()]``. ``TStateHolder`` must match the target
    //! computation's state type.
    template <class TStateHolder>
    TFuture<TJoinedStateKeyClient<TStateHolder>> CreateJoinedStateKeyClient() const
    {
        return CreateJoinedStateKeyProvider(&New<TYsonSerializableStateHolder<TStateHolder>>)
            .AsUnique()
            .Apply(BIND([] (TErrorOr<IJoinedStateKeyProviderPtr>&& result) {
                return TJoinedStateKeyClient<TStateHolder>(std::move(result.ValueOrThrow()));
            }));
    }

    template <class TStateHolder>
    void InitClient(TJoinedStateKeyClient<TStateHolder>& client) const
    {
        client = NConcurrency::WaitFor(CreateJoinedStateKeyClient<TStateHolder>()).ValueOrThrow();
    }

    template <class TStateHolder>
    void InitClient(TJoinedStateKeyClient<TStateHolder>& client, TStringBuf name) const
    {
        WithPrefix(name)->InitClient<TStateHolder>(client);
    }

    //! Looks up an external state manager declared in
    //! ``TComputationSpec::ExternalStateManagers[GetPrefix()]`` and wraps it in a typed key
    //! client. Throws if the prefix is not declared, or if the registered state class does
    //! not match ``TStateHolder`` (validated by ``IExternalStateManager::ValidateStateClass``).
    template <class TStateHolder>
    void InitExternalStateClient(TMutableStateKeyClient<TStateHolder>& client) const
    {
        auto manager = GetExternalStateManagerOrThrow(GetPrefix());
        manager->ValidateStateClass(typeid(TStateHolder));
        client = TMutableStateKeyClient<TStateHolder>(std::move(manager));
    }

    template <class TStateHolder>
    void InitExternalStateClient(TJoinedStateKeyClient<TStateHolder>& client) const
    {
        auto joiner = GetExternalStateJoinerOrThrow(GetPrefix());
        joiner->ValidateStateClass(typeid(TStateHolder));
        client = TJoinedStateKeyClient<TStateHolder>(std::move(joiner));
    }

    //! Convenience wrapper: ``InitExternalStateClient(client, name)`` is equivalent to
    //! ``WithPrefix(name)->InitExternalStateClient(client)``. Looks up the manager/joiner
    //! under the resulting prefix (i.e., ``GetPrefix() + normalized(name)``).
    template <class TStateHolder>
    void InitExternalStateClient(TMutableStateKeyClient<TStateHolder>& client, TStringBuf name) const
    {
        WithPrefix(name)->InitExternalStateClient<TStateHolder>(client);
    }

    template <class TStateHolder>
    void InitExternalStateClient(TJoinedStateKeyClient<TStateHolder>& client, TStringBuf name) const
    {
        WithPrefix(name)->InitExternalStateClient<TStateHolder>(client);
    }

    virtual TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const = 0;
    virtual TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const = 0;

    virtual IInitContextPtr AsPartition() const = 0;
    virtual IInitContextPtr AsKey(TKey key) const = 0;

    virtual IJobInitContextPtr WithPrefix(TStringBuf prefix) const = 0;
    virtual const std::string& GetPrefix() const = 0;

protected:
    virtual IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const = 0;
    virtual IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobInitContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
