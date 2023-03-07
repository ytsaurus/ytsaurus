#pragma once

#include "public.h"

#include <yt/server/master/object_server/object.h>
#include <yt/server/master/security_server/acl.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

/// Represents a storage type (e.g. HDD, SSD, RAM).
class TMedium
    : public NObjectServer::TObject
    , public TRefTracked<TMedium>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(int, Priority, MediumDefaultPriority);
    DEFINE_BYVAL_RW_PROPERTY(bool, Transient, false);
    DEFINE_BYVAL_RW_PROPERTY(bool, Cache, false);
    // TODO(savrus): Switch to BYVAL when generic property getter will return reference.
    DEFINE_BYREF_RW_PROPERTY(TMediumConfigPtr, Config);
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TTagId, ProfilingTag, -1);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TMedium(TMediumId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
