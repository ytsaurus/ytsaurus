#pragma once

#ifndef KEY_INL_H_
    #error "Direct inclusion of this file is not allowed, include helpers.h"
    // For the sake of sane code completion.
    #include "key.h"
#endif

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
TKey MakeKey(Ts&&... values)
{
    return TKey(MakeCompactUnversionedOwningRow(std::forward<Ts>(values)...));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
