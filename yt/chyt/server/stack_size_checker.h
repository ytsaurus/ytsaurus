#pragma once

// This method is a workaround around weird linker behavior.
// If nothing is called from this TU by main TU, our implementation
// of checkStackSize() is not being linked into resulting binary.
int IgnoreMe();
