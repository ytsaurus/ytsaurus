#pragma once

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <optional>
#include <string>
#include <typeinfo>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Helper for ``TExternalStateManagerBase::GetClient``; out-of-line so the templated base
//! does not pull #IYTClientProvider / #IClientsCache / #CreateRetryableClient() deps.
NApi::IClientPtr ResolveExternalStateManagerClient(const TExternalStateManagerContextPtr& context);

//! Helper for ``TExternalStateJoinerBase::GetClient``; see comment above.
NApi::IClientPtr ResolveExternalStateJoinerClient(
    const TExternalStateJoinerContextPtr& context,
    std::optional<std::string> cluster);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Base class for external state managers — analogue of #TSinkBase. Parses typed
//! parameters and dynamic parameters from the spec via ``TRegistry``, exposes them via
//! #GetParameters() / #GetDynamicParameters() (defined by EXTEND macros in the
//! derived class) and updates the dynamic-side atomically on Reconfigure.
template <class TStateImpl>
class TExternalStateManagerBase
    : public IExternalStateManager
{
public:
    //! Payload carried by the state — what the typed client is parameterized by.
    using TState = TStateImpl;
    //! Refcounted #IStateHolder object that carries the payload; handed out by #GetState()
    //! and stored by the manager internally.
    using TStateHolder = ::NYT::NFlow::TStateHolder<TStateImpl>;
    using TStateHolderPtr = TIntrusivePtr<TStateHolder>;

    TExternalStateManagerBase(
        TExternalStateManagerContextPtr context,
        TDynamicExternalStateManagerContextPtr dynamicContext);

    void ValidateStateClass(const std::type_info& expectedStateType) const final;

    TParametersPtr GetParametersBase() const final;
    TDynamicParametersPtr GetDynamicParametersBase() const final;

    NTableClient::TTableSchemaPtr GetKeySchema() const final;

protected:
    TExternalStateManagerContextPtr GetContext() const;
    TDynamicExternalStateManagerContextPtr GetDynamicContext() const;
    TExternalStateManagerSpecPtr GetSpec() const;
    TDynamicExternalStateManagerSpecPtr GetDynamicSpec() const;

    //! Returns a retryable-wrapped YT client for the pipeline cluster. Manager state always
    //! lives on the pipeline cluster — cross-cluster managers are intentionally not supported
    //! (exactly-once reasons).
    NApi::IClientPtr GetClient() const;

private:
    const TExternalStateManagerContextPtr Context_;
    const TParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicExternalStateManagerContext> DynamicContext_;
    TAtomicIntrusivePtr<TDynamicParameters> DynamicParameters_;
};

////////////////////////////////////////////////////////////////////////////////

//! Join-side counterpart of #TExternalStateManagerBase.
template <class TStateImpl>
class TExternalStateJoinerBase
    : public IExternalStateJoiner
{
public:
    using TState = TStateImpl;
    using TStateHolder = ::NYT::NFlow::TStateHolder<TStateImpl>;
    using TStateHolderPtr = TIntrusivePtr<TStateHolder>;

    TExternalStateJoinerBase(
        TExternalStateJoinerContextPtr context,
        TDynamicExternalStateJoinerContextPtr dynamicContext);

    void ValidateStateClass(const std::type_info& expectedStateType) const final;

    TParametersPtr GetParametersBase() const final;
    TDynamicParametersPtr GetDynamicParametersBase() const final;

    NTableClient::TTableSchemaPtr GetKeySchema() const final;
    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const final;
    const IPayloadConverterCachePtr& GetConverterCache() const final;
    bool HasKeySchemaOverride() const final;

protected:
    TExternalStateJoinerContextPtr GetContext() const;
    TDynamicExternalStateJoinerContextPtr GetDynamicContext() const;
    TExternalStateJoinerSpecPtr GetSpec() const;
    TDynamicExternalStateJoinerSpecPtr GetDynamicSpec() const;

    //! Resolves a retryable-wrapped YT client for the joiner.
    //!
    //! Resolution priority:
    //!   1. ``GetSpec()->ClientProviderResourceId`` — that resource's
    //!      ``IYTClientProvider::Get()`` (|cluster| argument is ignored).
    //!   2. ``GetSpec()->ClientFactoryResourceId`` — that resource's
    //!      ``IYTClientProvider::GetClient(cluster.value_or(PipelinePath.GetCluster()))``.
    //!   3. Default — ``ClientsCache->GetClient(cluster.value_or(PipelinePath.GetCluster()))``.
    //!
    //! All three return values are wrapped in #IRetryableClient.
    NApi::IClientPtr GetClient(std::optional<std::string> cluster = {}) const;

private:
    const TExternalStateJoinerContextPtr Context_;
    const TParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicExternalStateJoinerContext> DynamicContext_;
    TAtomicIntrusivePtr<TDynamicParameters> DynamicParameters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define EXTERNAL_STATE_MANAGER_BASE_INL_H_
#include "external_state_manager_base-inl.h"
#undef EXTERNAL_STATE_MANAGER_BASE_INL_H_
