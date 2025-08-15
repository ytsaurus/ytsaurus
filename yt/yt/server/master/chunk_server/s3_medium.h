#pragma once

#include "public.h"

#include "offshore_medium_base.h"

#include <yt/yt/ytlib/chunk_client/config.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TS3Medium
    : public TOffshoreMedium
{
public:
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::TS3MediumConfigPtr, Config, New<NChunkClient::TS3MediumConfig>());

    //! Some of the config fields must not be changed and some can be changed only under certain conditions, so
    //! this function checks such conditions and returns an error if it cannot apply the #newConfig.
    //! Usage of this function is not enforced - the Config field can be written to directly - but highly
    //! recommended to avoid serious problems. The only exception is the creation of the medium.
    TError TryUpdateConfig(
        NChunkClient::TS3MediumConfigPtr newConfig,
        const NSecurityServer::ISecurityManagerPtr& securityManager);

public:
    using TOffshoreMedium::TOffshoreMedium;

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
