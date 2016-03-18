#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Returns a factory for creating an ephemeral (non-persistent, in memory) YTree.
INodeFactoryPtr CreateEphemeralNodeFactory(bool shouldHideAttributes = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

