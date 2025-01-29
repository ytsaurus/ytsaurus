#pragma once

#include <variant>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

std::monostate RequireYtExecutorLink();

// Enforces RequireYtExecutorLink linkage, so targets including this header must link
// yt executor library.
inline std::monostate RequireYtExecutorLinkVar = RequireYtExecutorLink();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
