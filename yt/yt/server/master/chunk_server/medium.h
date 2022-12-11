#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

/// Represents a storage type (e.g. HDD, SSD, RAM).
class TMedium
    : public NObjectServer::TObject
    , public TRefTracked<TMedium>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYVAL_RW_PROPERTY(int, Index, -1);
    DEFINE_BYVAL_RW_PROPERTY(int, Priority, MediumDefaultPriority);
    DEFINE_BYVAL_RW_PROPERTY(bool, Transient, false);
    // TODO(savrus): Switch to BYVAL when generic property getter will return reference.
    DEFINE_BYREF_RW_PROPERTY(TMediumConfigPtr, Config, New<TMediumConfig>());

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYREF_RW_PROPERTY(std::optional<std::vector<TString>>, DiskFamilyWhitelist);

public:
    explicit TMedium(TMediumId id);

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TMedium)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
