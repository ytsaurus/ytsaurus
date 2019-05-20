#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/misc/core_dumper.h>

#include <util/stream/input.h>
#include <util/stream/file.h>

namespace NYT::NCoreDump {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, NYson::IYsonConsumer* consumer);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

i64 WriteSparseCoreDump(IInputStream* in, TFile* out);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
