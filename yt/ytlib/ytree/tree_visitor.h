#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    IYsonConsumer* consumer,
    bool withAttributes = true,
    const std::vector<Stroka>* const attributesToVisit = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
