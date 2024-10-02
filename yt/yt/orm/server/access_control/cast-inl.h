#ifndef CAST_INL_H_
#error "Direct inclusion of this file is not allowed, include cast.h"
// For the sake of sane code completion.
#include "cast.h"
#endif

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoAccessControlEntry>
void FromProto(
    TAccessControlEntry* entry,
    const TProtoAccessControlEntry& protoEntry)
{
    entry->Action = CheckedEnumCast<EAccessControlAction>(protoEntry.action());
    NYT::FromProto(&entry->Permissions, protoEntry.permissions());
    NYT::FromProto(&entry->Subjects, protoEntry.subjects());
    NYT::FromProto(&entry->Attributes, protoEntry.attributes());
}

template <class TProtoAccessControlEntry>
void ToProto(
    TProtoAccessControlEntry* protoEntry,
    const TAccessControlEntry& entry)
{
    using TProtoAccessControlAction = decltype(protoEntry->action());
    protoEntry->set_action(static_cast<TProtoAccessControlAction>(entry.Action));
    NYT::ToProto(protoEntry->mutable_permissions(), entry.Permissions);
    NYT::ToProto(protoEntry->mutable_subjects(), entry.Subjects);
    NYT::ToProto(protoEntry->mutable_attributes(), entry.Attributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
