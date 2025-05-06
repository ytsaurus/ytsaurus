#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyObject
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Address);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastPersistentHeartbeatTime);
    DEFINE_BYVAL_RW_PROPERTY(NSequoiaClient::ESequoiaReign, SequoiaReign);
    DEFINE_BYVAL_RW_PROPERTY(std::string, Version);

public:
    using NObjectServer::TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
