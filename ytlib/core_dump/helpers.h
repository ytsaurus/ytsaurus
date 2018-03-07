#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

namespace NYT {
namespace NCoreDump {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, NYson::IYsonConsumer* consumer);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
