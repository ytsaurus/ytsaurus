#pragma once

#include <yql/essentials/sql/v1/complete/name/schema/schema.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

    ISchema::TPtr MakeStaticSchema(THashMap<TPath, TVector<TFolderEntry>> data);

} // namespace NSQLComplete
