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

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    TTabletStatistics GetTabletStatistics() const override;

    void ValidateReshard() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
