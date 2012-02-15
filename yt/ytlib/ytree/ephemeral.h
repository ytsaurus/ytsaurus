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
INodeFactory* GetEphemeralNodeFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

