#pragma once

#include "public.h"

#include "config.h"
#include "medium_base.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TS3Medium
    : public TMedium
{
public:
    DEFINE_BYREF_RW_PROPERTY(TS3MediumConfigPtr, Config, New<TS3MediumConfig>());

public:
    using TMedium::TMedium;

    bool IsDomestic() const override;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TS3Medium, 0x61f4b09d);
};

DEFINE_MASTER_OBJECT_TYPE(TS3Medium)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
