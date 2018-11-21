#pragma once

#include "ypath_service.h"

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    NYson::IYsonConsumer* consumer,
    bool stable,
    const TNullable<std::vector<TString>>& attributeKeys = Null);

void VisitTree(
    INodePtr root,
    NYson::IAsyncYsonConsumer* consumer,
    bool stable,
    const TNullable<std::vector<TString>>& attributeKeys = Null);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
