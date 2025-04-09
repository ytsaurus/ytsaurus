#include "chaos_object_base.h"

#include "serialize.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TCoordinatorInfo::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, State);
}

void TMigration::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, OriginCellId);
    Persist(context, ImmigratedToCellId);
    Persist(context, EmigratedFromCellId);
    Persist(context, ImmigrationTime);
    Persist(context, EmigrationTime);
}

////////////////////////////////////////////////////////////////////////////////

void TChaosObjectBase::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Coordinators_);
    Save(context, Migration_);
}

void TChaosObjectBase::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Coordinators_);
    Load(context, Migration_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
