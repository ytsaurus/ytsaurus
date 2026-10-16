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
    Reconfigured_.Fire(configs...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
