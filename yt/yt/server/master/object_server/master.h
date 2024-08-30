#pragma once

#include "public.h"
#include "object_detail.h"
#include "type_handler_detail.h"

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TMasterObject
    : public TObject
{
public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
