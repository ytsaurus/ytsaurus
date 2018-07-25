#pragma once

#include "public.h"

#include <yt/core/yson/public.h>
#include <yt/core/ytree/public.h>

#include <yt/core/misc/ref.h>

#include <util/stream/output.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

void ParseFromMsgpack(const TSharedRef& buffer, NYson::IYsonConsumer* consumer);

NYTree::INodePtr ParseFromMsgpack(const TSharedRef& buffer);

TSharedRef SerializeToMsgpack(const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
