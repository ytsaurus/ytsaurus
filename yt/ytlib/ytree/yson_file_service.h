#pragma once

#include "common.h"
#include "ypath_service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Creates a service for performing simple, non-cached YPath
//! requests to a given file.
NYTree::TYPathServiceProducer::TPtr CreateYsonFileProducer(const Stroka& fileName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

