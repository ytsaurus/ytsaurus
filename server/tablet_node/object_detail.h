#pragma once

#include "public.h"

#include <yt/server/hydra/entity_map.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class IObjectBase
    : public NHydra::TEntityBase
{
public:
    const NObjectClient::TObjectId& GetId() const;

protected:
    explicit IObjectBase(const NObjectClient::TObjectId& id);

    const NObjectClient::TObjectId Id_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
