#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/entity_map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
    : public NHydra::TEntityBase
{
public:
    explicit TObjectBase(NObjectClient::TObjectId id);

    NObjectClient::TObjectId GetId() const;

protected:
    const NObjectClient::TObjectId Id_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
