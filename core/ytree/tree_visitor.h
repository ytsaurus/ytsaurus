#pragma once

#include "ypath_service.h"

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    NYson::IYsonConsumer* consumer,
    const TNullable<std::vector<TString>>& attributeKeys = Null,
    bool stable = false);

void VisitTree(
    INodePtr root,
    NYson::IAsyncYsonConsumer* consumer,
    const TNullable<std::vector<TString>>& attributeKeys = Null,
    bool stable = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
