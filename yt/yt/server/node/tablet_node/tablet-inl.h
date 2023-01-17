#ifndef TABLET_INL_H_
#error "Direct inclusion of this file is not allowed, include tablet.h"
// For the sake of sane code completion.
#include "tablet.h"
#endif

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
void TTabletErrors::ForEachError(TCallback&& callback) const
{
    for (auto key : TEnumTraits<NTabletClient::ETabletBackgroundActivity>::GetDomainValues()) {
        callback(BackgroundErrors[key].Load());
    }
    callback(ConfigError.Load());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
