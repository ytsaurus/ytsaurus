#pragma once

#include "object.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/acl.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! A schema (i.e. metaclass) object.
class TSchemaObject
    : public TObject
{
public:
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    using TObject::TObject;
    explicit TSchemaObject(TObjectId id);

private:
    PHOENIX_DECLARE_TYPE(TSchemaObject, 0xbfa0f5e8);
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateSchemaTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
