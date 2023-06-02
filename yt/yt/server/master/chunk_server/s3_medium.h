#pragma once

#include "public.h"

#include "config.h"
#include "medium_base.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TS3Medium
    : public TMediumBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(TS3MediumConfigPtr, Config, New<TS3MediumConfig>());

public:
    using TMediumBase::TMediumBase;

    bool IsDomestic() const override;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

DEFINE_MASTER_OBJECT_TYPE(TS3Medium)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
