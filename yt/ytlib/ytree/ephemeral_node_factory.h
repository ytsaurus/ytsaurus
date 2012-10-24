#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Returns a factory for creating an ephemeral (non-persistent, in memory) YTree.
/*!
 *  \note
 *  This factory is a singleton so returning a raw pointer is OK.
 */
INodeFactoryPtr GetEphemeralNodeFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

