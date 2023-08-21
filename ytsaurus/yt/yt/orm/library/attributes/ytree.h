#pragma once

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

//! Walks the #value deeply, i.e. ignoring non-existing children and list indexes.
//! Throws on the try of walking down the non-composite node.
NYTree::INodePtr GetNodeByPathOrEntity(
    const NYTree::INodePtr& value,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
