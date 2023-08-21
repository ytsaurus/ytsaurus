#pragma once

#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Evaluate constant string expression and return its value.
//! Throws an error if expression is not constant or result value is not a string.
TString EvaluateStringExpression(const DB::ASTPtr& expr, DB::ContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
