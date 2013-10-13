#pragma once

#include "public.h"

#include <core/misc/property.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public TNonCopyable
{
    DEFINE_BYVAL_RO_PROPERTY(TTabletId, Id);

public:
    explicit TTablet(const TTabletId& id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
