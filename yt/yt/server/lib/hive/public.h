#pragma once

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;

using NHiveClient::TMessageId;

using TAvenueEndpointId = NObjectClient::TObjectId;
using TEndpointId = NObjectClient::TObjectId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IHiveManager)
DECLARE_REFCOUNTED_STRUCT(IAvenueDirectory)

DECLARE_REFCOUNTED_CLASS(TSimpleAvenueDirectory)
DECLARE_REFCOUNTED_CLASS(TLogicalTimeRegistry)

class TMailbox;
DECLARE_ENTITY_TYPE(TCellMailbox, TCellId, ::THash<TCellId>)
DECLARE_ENTITY_TYPE(TAvenueMailbox, TAvenueEndpointId, ::THash<TAvenueEndpointId>)

DECLARE_REFCOUNTED_STRUCT(TMailboxRuntimeData)

constexpr int TypicalMailboxCount = 16;
using TMailboxList = TCompactVector<TMailbox*, TypicalMailboxCount>;

YT_DEFINE_STRONG_TYPEDEF(TLogicalTime, i64);

struct TPersistentMailboxState;

DECLARE_REFCOUNTED_STRUCT(TSerializedMessage)

DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TLogicalTimeRegistryConfig)
DECLARE_REFCOUNTED_CLASS(THiveManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
