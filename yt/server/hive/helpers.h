#pragma once

#include "public.h"

#include <yt/ytlib/hive/hive_service.pb.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

NHiveClient::NProto::TEncapsulatedMessage SerializeMessage(const ::google::protobuf::MessageLite& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
