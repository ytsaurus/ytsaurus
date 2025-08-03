#pragma once

#include "../fwd.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRawStateStore
    : public NYT::TRefCounted
{
public:
    virtual void* GetStateRaw(const void* key) = 0;

    template <typename TKey, typename TState>
    Y_FORCE_INLINE TStateStore<TKey, TState>* Upcast()
    {
        return reinterpret_cast<TStateStore<TKey, TState>*>(this);
    }
};

DEFINE_REFCOUNTED_TYPE(IRawStateStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
