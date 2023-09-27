#pragma once

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectLock
{
    NObjectClient::TObjectId NodeId;
    NHydra::TRevision Revision = NHydra::NullRevision;
};

void Serialize(const TObjectLock& lock, NYson::IYsonConsumer* consumer);
void Deserialize(TObjectLock& lock, const NYTree::INodePtr& node);
void Deserialize(TObjectLock& lock, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
