#pragma once

#include "object.h"

#include <server/security_server/acl.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! A schema (i.e. metaclass) object.
class TSchemaObject
    : public TNonversionedObjectBase
{
public:
    explicit TSchemaObject(const TObjectId& id);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateSchemaProxy(NCellMaster::TBootstrap* bootstrap, TSchemaObject* object);
IObjectTypeHandlerPtr CreateSchemaTypeHandler(NCellMaster::TBootstrap* bootstrap, EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
