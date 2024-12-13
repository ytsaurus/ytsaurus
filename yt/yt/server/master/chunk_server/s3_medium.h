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
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::TS3MediumConfigPtr, Config, New<NChunkClient::TS3MediumConfig>());

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
