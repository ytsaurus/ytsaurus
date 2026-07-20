#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Init-time context handed to IProcessFunction::Init. Creates the function's state key
//! clients and exposes its static parameters.
struct IRuntimeInitContext
    : public TRefCounted
{
public:
    template <class TStateHolder>
    TFuture<TMutableStateKeyClient<TStateHolder>> CreateMutableStateKeyClient(TStringBuf name) const;

    template <class TStateHolder>
    TFuture<TMutableStateKeyClient<TStateHolder>> CreateMutableStateKeyClient() const;

    template <class TStateHolder>
    void InitClient(TMutableStateKeyClient<TStateHolder>& client) const;

    template <class TStateHolder>
    void InitClient(TMutableStateKeyClient<TStateHolder>& client, TStringBuf name) const;

    //! Read-only join over another computation's internal state, declared in
    //! ``TComputationSpec::StateJoiners[GetPrefix()]``. ``TStateHolder`` must match the target
    //! computation's state type.
    template <class TStateHolder>
    TFuture<TJoinedStateKeyClient<TStateHolder>> CreateJoinedStateKeyClient() const;

    template <class TStateHolder>
    void InitClient(TJoinedStateKeyClient<TStateHolder>& client) const;

    template <class TStateHolder>
    void InitClient(TJoinedStateKeyClient<TStateHolder>& client, TStringBuf name) const;

    //! Looks up an external state manager declared in
    //! ``TComputationSpec::ExternalStateManagers[GetPrefix()]`` and wraps it in a typed key
    //! client. Throws if the prefix is not declared, or if the registered state class does
    //! not match ``TStateHolder``.
    template <class TStateHolder>
    void InitExternalStateClient(TMutableStateKeyClient<TStateHolder>& client) const;

    template <class TStateHolder>
    void InitExternalStateClient(TJoinedStateKeyClient<TStateHolder>& client) const;

    template <class TStateHolder>
    void InitExternalStateClient(TMutableStateKeyClient<TStateHolder>& client, TStringBuf name) const;

    template <class TStateHolder>
    void InitExternalStateClient(TJoinedStateKeyClient<TStateHolder>& client, TStringBuf name) const;

    //! Deserializes the static ``function_parameters`` block of the computation spec into
    //! the user's YSON struct |T| (defaults applied if the block is absent).
    template <class T>
    TIntrusivePtr<T> GetParameters() const;

    //! Raw ``function_parameters`` map from the static computation spec (never null; an empty
    //! map when the block is absent). Prefer the typed GetParameters<T>() helper.
    virtual NYTree::IMapNodePtr GetParametersNode() const = 0;

    //! Returns a resource the hosting computation declared in its
    //! ``required_resource_ids`` (worker side). Throws if the resource is not found there.
    //! String literals convert implicitly (TResourceId is a semi-strong typedef).
    virtual IResourcePtr GetStaticResource(const TResourceId& resourceId) const = 0;

    virtual TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const = 0;
    virtual TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const = 0;

    virtual IInitContextPtr AsPartition() const = 0;
    virtual IInitContextPtr AsKey(TKey key) const = 0;

    virtual IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const = 0;
    virtual const std::string& GetPrefix() const = 0;

protected:
    virtual IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const = 0;
    virtual IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRuntimeInitContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define RUNTIME_INIT_CONTEXT_INL_H_
#include "runtime_init_context-inl.h"
#undef RUNTIME_INIT_CONTEXT_INL_H_
