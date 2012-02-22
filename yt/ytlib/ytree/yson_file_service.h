#pragma once

#include "common.h"
#include "ypath_service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Creates a provider for performing simple, non-cached YPath
//! requests to a given file.
NYTree::TYPathServiceProducer CreateYsonFileProvider(const Stroka& fileName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
