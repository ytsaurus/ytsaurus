#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
    : public NHydra::TEntityBase
{
public:
    const NObjectClient::TObjectId& GetId() const;

protected:
    explicit TObjectBase(const NObjectClient::TObjectId& id);

    const NObjectClient::TObjectId Id_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
