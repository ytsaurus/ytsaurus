#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema_gateway.h>

namespace NSQLComplete {

    THolder<ISchemaGateway> MakeStaticSchemaGateway(THashMap<TString, TVector<TFolderEntry>> fs);

} // namespace NSQLComplete
