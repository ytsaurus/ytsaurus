#pragma once

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsConcat();
void RegisterTableFunctionsListDir();
void RegisterTableFunctionYtListLogTables();
void RegisterTableFunctionYtNodeAttributes();
void RegisterTableFunctionYtSecondaryQuery();
void RegisterTableFunctionYtTables();

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctions();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
