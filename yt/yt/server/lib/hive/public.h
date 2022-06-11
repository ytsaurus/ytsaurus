#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/misc/enum.h>
#include <yt/yt/core/misc/compact_vector.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;

using NHiveClient::TMessageId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THiveManager)

DECLARE_ENTITY_TYPE(TMailbox, TCellId, ::THash<TCellId>)
DECLARE_REFCOUNTED_STRUCT(TMailboxRuntimeData)

constexpr int TypicalMailboxCount = 16;
using TMailboxList = TCompactVector<TMailbox*, TypicalMailboxCount>;

DECLARE_REFCOUNTED_STRUCT(TSerializedMessage)

DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(THiveManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
