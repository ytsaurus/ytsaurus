#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/proto/watch_record.pb.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TTagSet FromProto(
    const NProto::TWatchRecord::TEvent::TChangedAttributeTags& attributeTags);

void ToProto(
    NProto::TWatchRecord::TEvent::TChangedAttributeTags& attributeTags,
    const TTagSet& tagsSet);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
