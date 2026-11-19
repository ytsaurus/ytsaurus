#pragma once

#ifndef RECONFIGURABLE_BASE_H_
    #error "Direct inclusion of this file is not allowed, include reconfigurable.h"
    // For the sake of sane code completion.
    #include "reconfigurable.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class... TConfigs>
void TReconfigurable<TConfigs...>::Reconfigure(const TIntrusivePtr<TConfigs>&... configs)
{
    try {
        Reconfigured_.Fire(configs...);
    } catch (const std::exception& ex) {
        OnReconfigureFail(TError(ex));
    }
}

template <class... TConfigs>
void TReconfigurable<TConfigs...>::OnReconfigureFail(const TError& /*error*/)
{
    // Called from the catch block above; preserve the original exception type.
    throw;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
