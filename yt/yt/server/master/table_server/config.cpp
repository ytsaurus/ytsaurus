#include "config.h"

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

void TTableManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("alert_on_master_table_schema_ref_counter_mismatch", &TThis::AlertOnMasterTableSchemaRefCounterMismatch)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
