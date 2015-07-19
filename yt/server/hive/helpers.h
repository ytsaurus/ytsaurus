#pragma once

#include "public.h"

#include <ytlib/hive/hive_service.pb.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

NProto::TEncapsulatedMessage SerializeMessage(const ::google::protobuf::MessageLite& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
