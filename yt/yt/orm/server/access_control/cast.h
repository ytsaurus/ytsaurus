#pragma once

#include "public.h"

#include <yt/yt/core/yson/protobuf_interop.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoAccessControlEntry>
void ToProto(
    TProtoAccessControlEntry* protoEntry,
    const TAccessControlEntry& entry);

template <class TProtoAccessControlEntry>
void FromProto(
    TAccessControlEntry* entry,
    const TProtoAccessControlEntry& protoEntry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl

#define CAST_INL_H_
#include "cast-inl.h"
#undef CAST_INL_H_
