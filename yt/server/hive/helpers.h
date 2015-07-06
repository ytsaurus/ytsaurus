#pragma once

#include "public.h"

#include <server/hive/hive_manager.pb.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

NProto::TEncapsulatedMessage SerializeMessage(const ::google::protobuf::MessageLite& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
