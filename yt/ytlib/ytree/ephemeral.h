#pragma once

#include "common.h"
#include "ytree.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Returns factory for creating an ephemeral (non-persistent, in memory) YTree.
INodeFactory* GetEphemeralNodeFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

