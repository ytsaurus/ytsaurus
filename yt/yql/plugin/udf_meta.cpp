#include "udf_meta.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NYqlPlugin {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TUdfModuleMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("functions", &TThis::Functions)
        .Default(GetEphemeralNodeFactory()->CreateList());
}

////////////////////////////////////////////////////////////////////////////////

void TUdfEntryMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("alias", &TThis::Alias);
    registrar.Parameter("updated_at", &TThis::UpdatedAt);
    registrar.Parameter("modules", &TThis::Modules)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUdfMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("udfs", &TThis::Udfs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
