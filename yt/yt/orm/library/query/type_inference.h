#pragma once

#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

//! Considers only builtin functions. Returns std::nullopt
//! if function is not found or has ambiguous return type.
std::optional<NTableClient::EValueType> TryInferFunctionReturnType(const std::string& functionName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
