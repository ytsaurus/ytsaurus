#pragma once

#include "common.h"
#include "ypath_service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Creates a service for performing simple, non-cached YPath
//! requests to a given file.
NYTree::TYPathServiceProvider CreateYsonFileProvider(const Stroka& fileName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
