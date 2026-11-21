#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/client/table_client/constrained_schema.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulNode
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NTableClient::ETableSchemaMode, SchemaMode, NTableClient::ETableSchemaMode::Weak);
    DEFINE_BYVAL_RW_PROPERTY(TMasterTableSchemaRawPtr, Schema);

public:
    virtual NSecurityServer::TAccount* GetAccount() const = 0;

    const NTableClient::TColumnStableNameToConstraintMap& GetConstraints() const;
    void SetConstraints(NTableClient::TColumnStableNameToConstraintMap constraints);

    // COMPAT(h0pless): This is a temporary workaround until schemaful node typehandler is introduced.
    virtual NObjectClient::TCellTag GetExternalCellTag() const = 0;
    virtual bool IsExternal() const = 0;

private:
    std::unique_ptr<NTableClient::TColumnStableNameToConstraintMap> Constraints_;

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TSchemafulNode, 0xaa6a56e6);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
