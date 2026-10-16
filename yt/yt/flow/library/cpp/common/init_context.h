#pragma once

#include "public.h"

#include "key.h"
#include "state.h"
#include "state_client.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IInitContext
    : public TRefCounted
{
    template <class TState_>
    TFuture<TMutableStateClient<TState_>> CreateMutableStateClient(TStringBuf name) const
    {
        return WithPrefix(name)->CreateMutableStateClient<TState_>();
    }

    template <class TState_>
    TFuture<TMutableStateClient<TState_>> CreateMutableStateClient() const
    {
        return CreateMutableStateProvider(&New<TYsonSerializableStateHolder<TState_>>)
            .AsUnique()
            .Apply(BIND([] (TErrorOr<IMutableStateProviderPtr>&& result) {
                return TMutableStateClient<TState_>(std::move(result.ValueOrThrow()));
            }));
    }

    template <class TState_>
    void InitClient(TMutableStateClient<TState_>& client) const
    {
        client = NConcurrency::WaitFor(CreateMutableStateClient<TState_>()).ValueOrThrow();
    }

    template <class TState_>
    void InitClient(TMutableStateClient<TState_>& client, TStringBuf name) const
    {
        WithPrefix(name)->InitClient<TState_>(client);
    }

    virtual TFuture<IMutableStateProviderPtr> CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const = 0;

    virtual IInitContextPtr WithPrefix(TStringBuf prefix) const = 0;
    virtual const std::string& GetPrefix() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInitContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
