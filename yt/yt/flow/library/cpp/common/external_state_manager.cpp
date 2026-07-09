#include "external_state_manager.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void IExternalStateManager::TParametersBase::Register(TRegistrar /*registrar*/)
{ }

void IExternalStateManager::TDynamicParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TFuture<IExternalStateManager::TListResult> IExternalStateManager::List(TFilter /*filter*/, i64 /*limit*/, std::optional<TKey> /*offsetExclusive*/)
{
    return MakeFuture<IExternalStateManager::TListResult>(TError("List is not implemented")
        << TErrorAttribute("type", TypeName(*this)));
}

////////////////////////////////////////////////////////////////////////////////

void IExternalStateJoiner::TParametersBase::Register(TRegistrar /*registrar*/)
{ }

void IExternalStateJoiner::TDynamicParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

bool IExternalStateJoiner::IsVisitorDriven() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IExternalStateJoiner::TListResult> IExternalStateJoiner::List(TFilter /*filter*/, i64 /*limit*/, std::optional<TKey> /*offsetExclusive*/)
{
    return MakeFuture<IExternalStateJoiner::TListResult>(TError("List is not implemented")
        << TErrorAttribute("type", TypeName(*this)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
