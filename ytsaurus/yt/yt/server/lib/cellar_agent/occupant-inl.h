#ifndef OCCUPANT_INL_H_
#error "Direct inclusion of this file is not allowed, include occupant.h"
// For the sake of sane code completion.
#include "occupant.h"
#endif

#include "occupier.h"

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TIntrusivePtr<T> ICellarOccupant::GetTypedOccupier() const
{
    auto occupier = GetOccupier();
    if (!occupier) {
        return nullptr;
    }

    YT_VERIFY(T::CellarType == occupier->GetCellarType());
    return StaticPointerCast<T>(std::move(occupier));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
