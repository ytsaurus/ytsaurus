#pragma once

#include "ypath_service.h"

#include <yt/core/yson/consumer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    NYson::IYsonConsumer* consumer,
    bool stable,
    const std::optional<std::vector<TString>>& attributeKeys = std::nullopt);

void VisitTree(
    INodePtr root,
    NYson::IAsyncYsonConsumer* consumer,
    bool stable,
    const std::optional<std::vector<TString>>& attributeKeys = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
