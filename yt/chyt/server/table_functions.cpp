#include "table_functions.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctions()
{
    RegisterTableFunctionsConcat();
    RegisterTableFunctionsListDir();
    RegisterTableFunctionYtListLogTables();
    RegisterTableFunctionYtNodeAttributes();
    RegisterTableFunctionYtQueueExports();
    RegisterTableFunctionYtSecondaryQuery();
    RegisterTableFunctionYtTables();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
