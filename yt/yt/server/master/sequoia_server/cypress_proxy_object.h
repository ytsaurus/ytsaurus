#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/sequoia_client/sequoia_reign.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyObject
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Address);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AliveUntil);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastPersistentHeartbeatTime);
    DEFINE_BYVAL_RW_PROPERTY(NSequoiaClient::ESequoiaReign, SequoiaReign);
    DEFINE_BYVAL_RW_PROPERTY(std::string, Version);

public:
    using NObjectServer::TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

private:
    PHOENIX_DECLARE_TYPE(TCypressProxyObject, 0xffcb6a34);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
