#pragma once

#include <yql/essentials/sql/v1/complete/name/object/simple/schema_gateway.h>

namespace NSQLComplete {

    ISimpleSchemaGateway::TPtr MakeStaticSimpleSchemaGateway(
        THashMap<TString, TVector<TFolderEntry>> fs);

} // namespace NSQLComplete
