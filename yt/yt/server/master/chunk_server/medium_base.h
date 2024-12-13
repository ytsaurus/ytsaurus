#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// Represents a storage type which is either subset of disks in YT
// or external storage system.
class TMedium
    : public NObjectServer::TObject
    , public TRefTracked<TMedium>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RW_PROPERTY(int, Index, -1);
    DEFINE_BYVAL_RW_PROPERTY(int, Priority, MediumDefaultPriority);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TMedium(TMediumId id);

    virtual bool IsDomestic() const = 0;
    bool IsOffshore() const;

    //! Should return type of the medium (domestic, S3, etc) in camel case.
    virtual std::string GetMediumType() const = 0;

    TDomesticMedium* AsDomestic();
    const TDomesticMedium* AsDomestic() const;

    virtual void FillMediumDescriptor(NChunkClient::NProto::TMediumDirectory::TMediumDescriptor* protoItem) const;

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
