#pragma once

#ifndef EXTERNAL_STATE_MANAGER_BASE_INL_H_
    #error "Direct inclusion of this file is not allowed, include external_state_manager_base.h"
    // For the sake of sane code completion.
    #include "external_state_manager_base.h"
#endif

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/misc/error.h>

#include <util/generic/typetraits.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TStateImpl>
TExternalStateManagerBase<TStateImpl>::TExternalStateManagerBase(
    TExternalStateManagerContextPtr context,
    TDynamicExternalStateManagerContextPtr dynamicContext)
    : Context_(std::move(context))
    , Parameters_(DynamicPointerCast<TParameters>(
        TRegistry::Get()->ParseExternalStateManagerParameters(Context_->ExternalStateManagerSpec)))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(DynamicPointerCast<TDynamicParameters>(
        TRegistry::Get()->ParseDynamicExternalStateManagerParameters(
            Context_->ExternalStateManagerSpec,
            dynamicContext->DynamicExternalStateManagerSpec)))
{
    SubscribeReconfigured(BIND([this] (const TDynamicExternalStateManagerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = DynamicPointerCast<TDynamicParameters>(
            TRegistry::Get()->ParseDynamicExternalStateManagerParameters(
                Context_->ExternalStateManagerSpec,
                dynamicContext->DynamicExternalStateManagerSpec));
    }));
}

template <class TStateImpl>
void TExternalStateManagerBase<TStateImpl>::ValidateStateClass(const std::type_info& expectedStateType) const
{
    if (expectedStateType != typeid(TStateImpl)) {
        THROW_ERROR_EXCEPTION(
            "External state class mismatch: manager exports %v, but the user code requested %v",
            TypeName<TStateImpl>(),
            TypeName(expectedStateType));
    }
}

template <class TStateImpl>
auto TExternalStateManagerBase<TStateImpl>::GetParametersBase() const -> TParametersPtr
{
    return Parameters_;
}

template <class TStateImpl>
auto TExternalStateManagerBase<TStateImpl>::GetDynamicParametersBase() const -> TDynamicParametersPtr
{
    return DynamicParameters_.Acquire();
}

template <class TStateImpl>
TExternalStateManagerContextPtr TExternalStateManagerBase<TStateImpl>::GetContext() const
{
    return Context_;
}

template <class TStateImpl>
TDynamicExternalStateManagerContextPtr TExternalStateManagerBase<TStateImpl>::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

template <class TStateImpl>
TExternalStateManagerSpecPtr TExternalStateManagerBase<TStateImpl>::GetSpec() const
{
    return Context_->ExternalStateManagerSpec;
}

template <class TStateImpl>
TDynamicExternalStateManagerSpecPtr TExternalStateManagerBase<TStateImpl>::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicExternalStateManagerSpec;
}

template <class TStateImpl>
NApi::IClientPtr TExternalStateManagerBase<TStateImpl>::GetClient() const
{
    return NDetail::ResolveExternalStateManagerClient(Context_);
}

template <class TStateImpl>
NTableClient::TTableSchemaPtr TExternalStateManagerBase<TStateImpl>::GetKeySchema() const
{
    return Context_->KeySchema;
}

////////////////////////////////////////////////////////////////////////////////

template <class TStateImpl>
TExternalStateJoinerBase<TStateImpl>::TExternalStateJoinerBase(
    TExternalStateJoinerContextPtr context,
    TDynamicExternalStateJoinerContextPtr dynamicContext)
    : Context_(std::move(context))
    , Parameters_(DynamicPointerCast<TParameters>(
        TRegistry::Get()->ParseExternalStateJoinerParameters(Context_->ExternalStateJoinerSpec)))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(DynamicPointerCast<TDynamicParameters>(
        TRegistry::Get()->ParseDynamicExternalStateJoinerParameters(
            Context_->ExternalStateJoinerSpec,
            dynamicContext->DynamicExternalStateJoinerSpec)))
{
    SubscribeReconfigured(BIND([this] (const TDynamicExternalStateJoinerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = DynamicPointerCast<TDynamicParameters>(
            TRegistry::Get()->ParseDynamicExternalStateJoinerParameters(
                Context_->ExternalStateJoinerSpec,
                dynamicContext->DynamicExternalStateJoinerSpec));
    }));
}

template <class TStateImpl>
void TExternalStateJoinerBase<TStateImpl>::ValidateStateClass(const std::type_info& expectedStateType) const
{
    if (expectedStateType != typeid(TStateImpl)) {
        THROW_ERROR_EXCEPTION(
            "External state class mismatch: joiner exports %v, but the user code requested %v",
            TypeName<TStateImpl>(),
            TypeName(expectedStateType));
    }
}

template <class TStateImpl>
auto TExternalStateJoinerBase<TStateImpl>::GetParametersBase() const -> TParametersPtr
{
    return Parameters_;
}

template <class TStateImpl>
auto TExternalStateJoinerBase<TStateImpl>::GetDynamicParametersBase() const -> TDynamicParametersPtr
{
    return DynamicParameters_.Acquire();
}

template <class TStateImpl>
TExternalStateJoinerContextPtr TExternalStateJoinerBase<TStateImpl>::GetContext() const
{
    return Context_;
}

template <class TStateImpl>
TDynamicExternalStateJoinerContextPtr TExternalStateJoinerBase<TStateImpl>::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

template <class TStateImpl>
TExternalStateJoinerSpecPtr TExternalStateJoinerBase<TStateImpl>::GetSpec() const
{
    return Context_->ExternalStateJoinerSpec;
}

template <class TStateImpl>
TDynamicExternalStateJoinerSpecPtr TExternalStateJoinerBase<TStateImpl>::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicExternalStateJoinerSpec;
}

template <class TStateImpl>
NApi::IClientPtr TExternalStateJoinerBase<TStateImpl>::GetClient(std::optional<std::string> cluster) const
{
    return NDetail::ResolveExternalStateJoinerClient(Context_, std::move(cluster));
}

template <class TStateImpl>
NTableClient::TTableSchemaPtr TExternalStateJoinerBase<TStateImpl>::GetKeySchema() const
{
    return Context_->KeySchema;
}

template <class TStateImpl>
const std::optional<THashSet<TStreamId>>& TExternalStateJoinerBase<TStateImpl>::GetKeyProviderStreams() const
{
    return Context_->ExternalStateJoinerSpec->JoinOn->KeyProviderStreams;
}

template <class TStateImpl>
const IPayloadConverterCachePtr& TExternalStateJoinerBase<TStateImpl>::GetConverterCache() const
{
    return Context_->ConverterCache;
}

template <class TStateImpl>
bool TExternalStateJoinerBase<TStateImpl>::HasKeySchemaOverride() const
{
    return Context_->ExternalStateJoinerSpec->JoinOn->KeySchemaOverride != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
