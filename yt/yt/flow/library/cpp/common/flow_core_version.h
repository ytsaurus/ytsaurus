#pragma once

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Resolves the FlowCoreVersion string for this binary.
//
// The result is a self-describing string of the form
//     "<hash> (commit hash)"
// or
//     "<checksum> (binary checksum)"
//
// Resolution policy: prefer the arc/git commit hash baked in at build
// time via GetProgramHash(); if it is missing, fall back to the binary
// file checksum.
const std::string& ResolveFlowCoreVersion();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
