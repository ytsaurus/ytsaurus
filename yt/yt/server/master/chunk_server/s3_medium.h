#pragma once

#include "public.h"

#include "medium_base.h"

#include <yt/yt/ytlib/chunk_client/config.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TS3Medium
    : public TMedium
{
public:
    DEFINE_BYREF_RO_PROPERTY_NO_INIT(NChunkClient::TS3MediumConfigPtr, Config);

    //! Some of the config fields must not be changed and some can be changed only under certain conditions, so
    //! this function checks such conditions and returns an error if it cannot apply the #newConfig.
    //! The only exception is when the TS3Medium is just constructed and has an empty config.
    TError TryUpdateConfig(
        NChunkClient::TS3MediumConfigPtr newConfig,
        const NSecurityServer::ISecurityManagerPtr& securityManager);

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

DEFINE_MASTER_OBJECT_TYPE(TS3Medium)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
