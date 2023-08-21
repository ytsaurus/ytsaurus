#include "table_functions.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctions()
{
    RegisterTableFunctionsConcat();
    RegisterTableFunctionsListDir();
    RegisterTableFunctionYtListLogTables();
    RegisterTableFunctionYtNodeAttributes();
    RegisterTableFunctionYtSecondaryQuery();
    RegisterTableFunctionYtTables();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
