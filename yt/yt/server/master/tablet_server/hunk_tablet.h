#pragma once

#include "tablet_base.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class THunkTablet
    : public TTabletBase
{
public:
    using TBase = TTabletBase;
    using TBase::TBase;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    TTabletStatistics GetTabletStatistics(bool fromAuxiliaryCell = false) const override;

    void ValidateReshard() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
