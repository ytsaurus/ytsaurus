#pragma once

#include "node.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Builds a factory which throws on every attempt to create a node.
std::unique_ptr<INodeFactory> CreateFaultyNodeFactory();

//! Returns a cached instance of faulty factory.
INodeFactory* GetFaultyNodeFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

