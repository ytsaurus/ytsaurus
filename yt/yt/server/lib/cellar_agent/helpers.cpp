#include "helpers.h"

#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellarAgent {

using namespace NObjectClient;
using namespace NCellarClient;

////////////////////////////////////////////////////////////////////////////////

ECellarType GetCellarTypeFromCellId(TCellId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::TabletCell:
            return ECellarType::Tablet;

        case EObjectType::ChaosCell:
            return ECellarType::Chaos;

        default:
            YT_ABORT();
    }
}

ECellarType GetCellarTypeFromCellBundleId(TObjectId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::TabletCellBundle:
            return ECellarType::Tablet;

        case EObjectType::ChaosCellBundle:
            return ECellarType::Chaos;

        default:
            YT_ABORT();
    }
}

const TString& GetCellCypressPrefix(TCellId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::TabletCell:
            return TabletCellCypressPrefix;

        case EObjectType::ChaosCell:
            return ChaosCellCypressPrefix;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
