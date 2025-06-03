#pragma once

#include "public.h"

#include "config.h"
#include "medium_base.h"

#include <yt/yt/core/misc/property.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

/// Represents a storage type (e.g. HDD, SSD, RAM).
class TDomesticMedium
    : public TMedium
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Transient, false);

    // TODO(savrus): Switch to BYVAL when generic property getter will return reference.
    DEFINE_BYREF_RW_PROPERTY(TDomesticMediumConfigPtr, Config, New<TDomesticMediumConfig>());

    DEFINE_BYREF_RW_PROPERTY(std::optional<std::vector<TString>>, DiskFamilyWhitelist);

    DEFINE_BYVAL_RW_PROPERTY(bool, EnableSequoiaReplicas, true);

public:
    using TMedium::TMedium;

    bool IsDomestic() const override;

    std::string GetMediumType() const override;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void FillMediumDescriptor(NChunkClient::NProto::TMediumDirectory::TMediumDescriptor* protoItem) const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;
};

DEFINE_MASTER_OBJECT_TYPE(TDomesticMedium)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
