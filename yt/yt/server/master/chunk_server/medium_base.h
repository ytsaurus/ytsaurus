#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// Represents a storage type which is either subset of disks in YT
// or external storage system.
class TMediumBase
    : public NObjectServer::TObject
    , public TRefTracked<TMediumBase>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYVAL_RW_PROPERTY(int, Index, -1);
    DEFINE_BYVAL_RW_PROPERTY(int, Priority, MediumDefaultPriority);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TMediumBase(TMediumId id);

    virtual bool IsDomestic() const = 0;
    bool IsOffshore() const;

    TDomesticMedium* AsDomestic();
    const TDomesticMedium* AsDomestic() const;

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
