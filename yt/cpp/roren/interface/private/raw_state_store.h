#pragma once

#include "../fwd.h"

#include <util/generic/ptr.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRawStateStore
    : public TThrRefBase
{
public:
    virtual void* GetState(const void* key) = 0;

    template <typename TKey, typename TState>
    Y_FORCE_INLINE TStateStore<TKey, TState>* Upcast()
    {
        return reinterpret_cast<TStateStore<TKey, TState>*>(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
