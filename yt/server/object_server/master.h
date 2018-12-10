#pragma once

#include "public.h"
#include "object_detail.h"
#include "type_handler_detail.h"

#include <yt/server/cell_master/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TMasterObject
    : public TNonversionedObjectBase
{
public:
    explicit TMasterObject(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
