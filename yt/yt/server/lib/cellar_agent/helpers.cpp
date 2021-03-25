#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellarAgent {

using namespace NObjectClient;
using namespace NCellarClient;

////////////////////////////////////////////////////////////////////////////////

ECellarType GetCellarTypeFromId(TCellId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::TabletCell:
            return ECellarType::Tablet;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
