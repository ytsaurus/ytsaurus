#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
    : public NHydra::TEntityBase
{
public:
    NObjectClient::TObjectId GetId() const;

protected:
    const NObjectClient::TObjectId Id_;

    explicit TObjectBase(NObjectClient::TObjectId id);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
