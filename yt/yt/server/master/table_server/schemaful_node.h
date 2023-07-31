#pragma once

#include "public.h"

#include <yt/yt/server/master/security_server/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct ISchemafulNode
{
    virtual ~ISchemafulNode() = default;

    virtual TMasterTableSchema* GetSchema() const = 0;
    virtual void SetSchema(TMasterTableSchema* schema) = 0;
    virtual NSecurityServer::TAccount* GetAccount() const = 0;

    // COMPAT(h0pless): This is a temporary workaround until schemaful node typehandler is introduced.
    virtual NObjectClient::TCellTag GetExternalCellTag() const = 0;
    virtual bool IsExternal() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
