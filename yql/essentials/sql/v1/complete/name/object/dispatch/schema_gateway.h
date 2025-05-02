#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema_gateway.h>

namespace NSQLComplete {

    ISchemaGateway::TPtr MakeDispatchSchemaGateway(THashMap<TString, ISchemaGateway::TPtr> mapping);

} // namespace NSQLComplete
