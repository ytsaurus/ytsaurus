#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
    : public NHydra::TEntityBase
{
public:
    NObjectClient::TObjectId GetId() const;

protected:
    explicit TObjectBase(NObjectClient::TObjectId id);

    const NObjectClient::TObjectId Id_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
