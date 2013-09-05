#pragma once

#include "attribute_provider.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    NYson::IYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter = TAttributeFilter::All,
    bool sortKeys = false,
    bool ignoreOpaque = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
