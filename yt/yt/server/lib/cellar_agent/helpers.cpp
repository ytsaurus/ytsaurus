#include "helpers.h"

#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NCellarAgent {

using namespace NObjectClient;
using namespace NCellarClient;
using namespace NYPath;

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

const TYPath& GetCellHydraPersistenceCypressPathPrefix(TCellId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::TabletCell:
            return TabletCellsHydraPersistenceCypressPrefix;

        case EObjectType::ChaosCell:
            return ChaosCellsHydraPersistenceCypressPrefix;

        default:
            YT_ABORT();
    }
}

TYPath GetCellHydraPersistencePath(NElection::TCellId id)
{
    return Format("%v/%v", GetCellHydraPersistenceCypressPathPrefix(id), id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
