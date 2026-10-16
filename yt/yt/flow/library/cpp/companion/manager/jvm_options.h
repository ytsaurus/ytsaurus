#pragma once

#include <string>
#include <vector>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Resolves effective JVM options by combining defaults with environment overrides.
std::vector<std::string> ResolveJvmOptions();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
